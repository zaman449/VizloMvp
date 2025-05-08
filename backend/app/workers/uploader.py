import asyncio
import logging
import os
from pathlib import Path
import mimetypes

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

# Default retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 1.0

# Known MIME types for HLS
HLS_MIME_TYPES = {
    ".m3u8": "application/vnd.apple.mpegurl",
    ".ts": "video/MP2T",
}

async def _upload_boto3_task(s3_client, local_file_path_str: str, bucket_name: str, s3_key: str, content_type: str):
    """Synchronous part of the upload, to be run in a thread."""
    s3_client.upload_file(
        local_file_path_str,
        bucket_name,
        s3_key,
        ExtraArgs={'ContentType': content_type}
    )

async def upload_file_with_retry(
    s3_client,
    local_file_path: Path,
    bucket_name: str,
    s3_key: str,
    content_type: str
):
    """Uploads a single file to S3 with retry logic, running sync boto3 calls in a thread."""
    current_retry = 0
    delay = INITIAL_BACKOFF_SECONDS
    last_exception = None
    while current_retry < MAX_RETRIES:
        try:
            logger.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key} (Attempt {current_retry + 1}/{MAX_RETRIES})")
            await asyncio.to_thread(
                _upload_boto3_task,
                s3_client,
                str(local_file_path),
                bucket_name,
                s3_key,
                content_type
            )
            logger.info(f"Successfully uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
            return
        except ClientError as e:
            logger.warning(f"ClientError during S3 upload of {s3_key} (Attempt {current_retry + 1}): {e}. Retrying...")
            last_exception = e
        except Exception as e:
            logger.warning(f"Unexpected error during S3 upload of {s3_key} (Attempt {current_retry + 1}): {e}. Retrying...")
            last_exception = e

        current_retry += 1
        if current_retry < MAX_RETRIES:
            logger.info(f"Waiting {delay} seconds before next retry for {s3_key}.")
            await asyncio.sleep(delay)
            delay *= 2
    
    logger.error(f"Failed to upload {s3_key} after {MAX_RETRIES} attempts.")
    raise RuntimeError(f"Failed to upload {s3_key} to S3 after {MAX_RETRIES} attempts. Last error: {last_exception}")

async def upload_dir_to_r2(local_dir: Path, remote_prefix: str) -> str:
    """
    Uploads all files from a local directory to Cloudflare R2 (S3-compatible).

    Args:
        local_dir: Path to the local directory containing files to upload.
        remote_prefix: The prefix for the S3 keys (e.g., "answers/<answer_id>").

    Returns:
        The public HTTPS URL for the 'master.m3u8' file.

    Raises:
        RuntimeError: If configuration is missing or upload fails persistently.
        FileNotFoundError: If local_dir does not exist.
    """
    if not local_dir.is_dir():
        raise FileNotFoundError(f"Local directory not found: {local_dir}")

    cf_r2_key = os.getenv("CF_R2_KEY")
    cf_r2_secret = os.getenv("CF_R2_SECRET")
    cf_r2_endpoint = os.getenv("CF_R2_ENDPOINT")
    cf_r2_bucket = os.getenv("CF_R2_BUCKET")
    cf_public_cdn = os.getenv("CF_PUBLIC_CDN")

    required_env_vars = {
        "CF_R2_KEY": cf_r2_key, "CF_R2_SECRET": cf_r2_secret,
        "CF_R2_ENDPOINT": cf_r2_endpoint, "CF_R2_BUCKET": cf_r2_bucket,
        "CF_PUBLIC_CDN": cf_public_cdn
    }
    missing_vars = [var_name for var_name, val in required_env_vars.items() if not val]
    if missing_vars:
        logger.error(f"Missing R2 configuration. Required env vars: {', '.join(missing_vars)}")
        raise RuntimeError(f"Missing R2 configuration. Required env vars: {', '.join(missing_vars)}")

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=cf_r2_key,
            aws_secret_access_key=cf_r2_secret,
            endpoint_url=cf_r2_endpoint,
        )
    except NoCredentialsError:
        logger.error("Boto3 NoCredentialsError: AWS credentials not found or incomplete for R2.")
        raise RuntimeError("AWS credentials not found or incomplete for R2 upload.")
    except Exception as e:
        logger.error(f"Error creating S3 client for R2: {e}")
        raise RuntimeError(f"Error creating S3 client for R2: {e}")

    # Upload files sequentially. For parallel uploads, create a list of tasks and use asyncio.gather.
    for local_file_path in local_dir.rglob('*'):
        if local_file_path.is_file():
            relative_path_to_file = local_file_path.relative_to(local_dir)
            s3_key = f"{remote_prefix.strip('/')}/{str(relative_path_to_file).replace('\\', '/')}"

            file_ext = local_file_path.suffix.lower()
            content_type = HLS_MIME_TYPES.get(file_ext)
            if not content_type:
                content_type, _ = mimetypes.guess_type(str(local_file_path))
                if not content_type:
                    content_type = 'application/octet-stream'
            
            await upload_file_with_retry(s3_client, local_file_path, cf_r2_bucket, s3_key, content_type)

    # Ensure master.m3u8 exists locally to form the URL (packager should guarantee this)
    expected_master_manifest_local_path = local_dir / "master.m3u8"
    if not expected_master_manifest_local_path.exists():
        logger.error(f"master.m3u8 not found in local HLS output directory: {local_dir}")
        raise RuntimeError(f"master.m3u8 not found in {local_dir}, cannot form public URL.")

    master_manifest_s3_key = f"{remote_prefix.strip('/')}/master.m3u8"
    public_url = f"{cf_public_cdn.strip('/')}/{master_manifest_s3_key}"
    
    logger.info(f"All files from {local_dir} uploaded to R2 prefix {remote_prefix}.")
    logger.info(f"Public HLS manifest URL: {public_url}")
    
    return public_url

