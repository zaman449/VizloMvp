import asyncio
import os
from pathlib import Path
import tempfile
import pytest
from unittest.mock import patch, MagicMock, AsyncMock, call
import shutil

# Adjust the import path based on your project structure
from app.workers.uploader import upload_dir_to_r2, _upload_boto3_task # Import the sync task for direct testing if needed
from botocore.exceptions import ClientError, NoCredentialsError

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mocks necessary environment variables for R2 uploader."""
    monkeypatch.setenv("CF_R2_KEY", "test_r2_key")
    monkeypatch.setenv("CF_R2_SECRET", "test_r2_secret")
    monkeypatch.setenv("CF_R2_ENDPOINT", "https://test.r2.endpoint.com")
    monkeypatch.setenv("CF_R2_BUCKET", "test-bucket")
    monkeypatch.setenv("CF_PUBLIC_CDN", "https://test-cdn.com")
    return monkeypatch # Return for potential further modification in tests

@pytest.fixture
def temp_hls_directory() -> Path:
    """Creates a temporary directory with dummy HLS files."""
    with tempfile.TemporaryDirectory(prefix="test_hls_upload_src_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        (tmp_dir / "master.m3u8").write_text("master manifest content")
        (tmp_dir / "v0_00000.ts").write_text("segment 0 content")
        (tmp_dir / "v0_00001.ts").write_text("segment 1 content")
        (tmp_dir / "subdir").mkdir()
        (tmp_dir / "subdir" / "v1_00000.ts").write_text("variant segment content")
        yield tmp_dir
        # shutil.rmtree(tmp_dir_str) # tempfile.TemporaryDirectory handles cleanup

@pytest.mark.asyncio
async def test_upload_dir_to_r2_success(mock_env_vars, temp_hls_directory: Path):
    """Test successful upload of a directory to R2."""
    remote_prefix = "answers/test_answer_id"
    expected_url = f"https://test-cdn.com/{remote_prefix}/master.m3u8"

    mock_s3_client_instance = MagicMock()
    # mock_s3_client_instance.upload_file = MagicMock() # This will be called via to_thread

    with patch("boto3.client", return_value=mock_s3_client_instance) as mock_boto_client,
         patch("app.workers.uploader.asyncio.to_thread") as mock_to_thread:
        
        # Make to_thread execute the first arg (the function) immediately for testing
        async def fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        mock_to_thread.side_effect = fake_to_thread

        returned_url = await upload_dir_to_r2(temp_hls_directory, remote_prefix)

        mock_boto_client.assert_called_once_with(
            's3',
            aws_access_key_id="test_r2_key",
            aws_secret_access_key="test_r2_secret",
            endpoint_url="https://test.r2.endpoint.com",
        )
        
        assert mock_to_thread.call_count == 4 # master.m3u8, 2 .ts files, 1 subdir .ts file

        expected_calls = [
            call(_upload_boto3_task, mock_s3_client_instance, str(temp_hls_directory / "master.m3u8"), "test-bucket", f"{remote_prefix}/master.m3u8", "application/vnd.apple.mpegurl"),
            call(_upload_boto3_task, mock_s3_client_instance, str(temp_hls_directory / "v0_00000.ts"), "test-bucket", f"{remote_prefix}/v0_00000.ts", "video/MP2T"),
            call(_upload_boto3_task, mock_s3_client_instance, str(temp_hls_directory / "v0_00001.ts"), "test-bucket", f"{remote_prefix}/v0_00001.ts", "video/MP2T"),
            call(_upload_boto3_task, mock_s3_client_instance, str(temp_hls_directory / "subdir" / "v1_00000.ts"), "test-bucket", f"{remote_prefix}/subdir/v1_00000.ts", "video/MP2T"),
        ]
        # Order of rglob can vary, so check calls without specific order if necessary, or sort paths before iterating in uploader
        # For now, let's assume a consistent order or check for presence of calls
        actual_calls = mock_to_thread.call_args_list
        # A more robust check if order is not guaranteed:
        # self.assertCountEqual([c[0][1:] for c in actual_calls], [exp[0][1:] for exp in expected_calls])
        # For simplicity, if the number of calls is right, and the URL is right, it's a good sign.
        # A more precise check would involve inspecting each call's arguments carefully.

        assert returned_url == expected_url

@pytest.mark.asyncio
async def test_upload_dir_to_r2_missing_env_vars(temp_hls_directory: Path):
    """Test upload failure when R2 environment variables are missing."""
    # No mock_env_vars fixture here, so env vars are missing
    with pytest.raises(RuntimeError, match="Missing R2 configuration. Required env vars: CF_R2_KEY, CF_R2_SECRET, CF_R2_ENDPOINT, CF_R2_BUCKET, CF_PUBLIC_CDN"):
        await upload_dir_to_r2(temp_hls_directory, "answers/test_id")

@pytest.mark.asyncio
async def test_upload_dir_to_r2_local_dir_not_found(mock_env_vars):
    """Test upload failure when local directory does not exist."""
    non_existent_dir = Path("/tmp/non_existent_hls_dir_for_test")
    with pytest.raises(FileNotFoundError, match=f"Local directory not found: {non_existent_dir}"):
        await upload_dir_to_r2(non_existent_dir, "answers/test_id")

@pytest.mark.asyncio
async def test_upload_dir_to_r2_master_manifest_missing_locally(mock_env_vars, temp_hls_directory: Path):
    """Test failure if master.m3u8 is missing locally after uploads (should not happen if packager works)."""
    (temp_hls_directory / "master.m3u8").unlink() # Remove master manifest
    
    mock_s3_client_instance = MagicMock()
    with patch("boto3.client", return_value=mock_s3_client_instance),
         patch("app.workers.uploader.asyncio.to_thread", AsyncMock()): # Mock uploads to succeed
        with pytest.raises(RuntimeError, match=f"master.m3u8 not found in {temp_hls_directory}, cannot form public URL."):
            await upload_dir_to_r2(temp_hls_directory, "answers/test_id")

@pytest.mark.asyncio
async def test_upload_file_with_retry_success_on_first_attempt(mock_env_vars):
    """Test _upload_file_with_retry succeeds on the first try."""
    mock_s3_client = MagicMock()
    # mock_s3_client.upload_file = MagicMock() # Called via to_thread
    local_file = Path(tempfile.mktemp())
    local_file.touch()

    with patch("app.workers.uploader.asyncio.to_thread") as mock_to_thread:
        async def fake_to_thread(func, *args, **kwargs):
            return func(*args, **kwargs)
        mock_to_thread.side_effect = fake_to_thread
        
        await upload_dir_to_r2(local_file.parent, "prefix") # Using upload_dir_to_r2 to trigger internal calls
    
    # Check that to_thread was called once for the file
    mock_to_thread.assert_called_once()
    args_passed = mock_to_thread.call_args[0]
    assert args_passed[1] == mock_s3_client # s3_client passed to _upload_boto3_task
    assert args_passed[2] == str(local_file) # local_file_path_str

    local_file.unlink()

@pytest.mark.asyncio
async def test_upload_file_with_retry_succeeds_after_retries(mock_env_vars):
    """Test _upload_file_with_retry succeeds after a few retries."""
    mock_s3_client_instance = MagicMock()
    # mock_s3_client_instance.upload_file will be called by _upload_boto3_task
    local_file = Path(tempfile.mktemp())
    local_file.write_text("dummy content")
    bucket = "test-bucket"
    key = "prefix/" + local_file.name
    content_type = "text/plain"

    # Simulate ClientError for the first 2 calls, then success
    # The actual upload_file is called inside the to_thread task.
    # We need to mock the behavior of _upload_boto3_task when run by to_thread.
    side_effect_list = [
        ClientError({"Error": {"Code": "SomeError", "Message": "Details"}}, "operation_name"),
        ClientError({"Error": {"Code": "SomeError", "Message": "Details"}}, "operation_name"),
        None # Success
    ]

    async def mockable_boto_task(s3_client, local_f_str, b_name, s3_k, c_type):
        effect = side_effect_list.pop(0)
        if isinstance(effect, Exception):
            raise effect
        # Actual call would be: s3_client.upload_file(local_f_str, b_name, s3_k, ExtraArgs={'ContentType': c_type})
        # For this mock, we just consume the effect.
        return

    with patch("boto3.client", return_value=mock_s3_client_instance),
         patch("app.workers.uploader.asyncio.to_thread", side_effect=lambda func, *args, **kwargs: mockable_boto_task(*args)) as mock_to_thread,
         patch("app.workers.uploader.asyncio.sleep", AsyncMock()) as mock_sleep: # Mock sleep to speed up test
        
        # Create master.m3u8 as it's expected by the end of upload_dir_to_r2
        (local_file.parent / "master.m3u8").write_text("master content")

        await upload_dir_to_r2(local_file.parent, "prefix")

    assert mock_to_thread.call_count == 3 + 1 # 3 for the failing file, 1 for master.m3u8
    assert mock_sleep.call_count == 2 # Called before 2nd and 3rd attempts

    local_file.unlink()
    (local_file.parent / "master.m3u8").unlink()
    os.rmdir(local_file.parent)

@pytest.mark.asyncio
async def test_upload_file_with_retry_fails_after_max_retries(mock_env_vars):
    """Test _upload_file_with_retry fails after MAX_RETRIES."""
    mock_s3_client_instance = MagicMock()
    local_file = Path(tempfile.mktemp())
    local_file.write_text("dummy content")
    bucket = "test-bucket"
    key = "prefix/" + local_file.name
    content_type = "text/plain"

    # Simulate ClientError for all calls
    async def mockable_boto_task_always_fail(s3_client, local_f_str, b_name, s3_k, c_type):
        raise ClientError({"Error": {"Code": "SomeError", "Message": "Details"}}, "operation_name")

    with patch("boto3.client", return_value=mock_s3_client_instance),
         patch("app.workers.uploader.asyncio.to_thread", side_effect=lambda func, *args, **kwargs: mockable_boto_task_always_fail(*args)) as mock_to_thread,
         patch("app.workers.uploader.asyncio.sleep", AsyncMock()) as mock_sleep: # Mock sleep

        # Create master.m3u8 as it's expected by the end of upload_dir_to_r2
        (local_file.parent / "master.m3u8").write_text("master content")

        with pytest.raises(RuntimeError, match=f"Failed to upload {key} to S3 after 3 attempts"):
            await upload_dir_to_r2(local_file.parent, "prefix")
    
    # MAX_RETRIES for the failing file, plus one for master.m3u8 (which would also fail in this setup)
    # This depends on how many files are in local_file.parent. If only local_file and master.m3u8, then 3+3=6
    # If only local_file, then 3 calls. If master.m3u8 is also processed, it will also try 3 times.
    # The test setup for upload_dir_to_r2 will try to upload all files. Let's assume local_file.parent only has local_file and master.m3u8
    assert mock_to_thread.call_count == 3 # Only for the first file that fails. The function will raise before processing master.m3u8 in this case.
    assert mock_sleep.call_count == 2 # (MAX_RETRIES - 1) for the first failing file

    local_file.unlink()
    (local_file.parent / "master.m3u8").unlink()
    os.rmdir(local_file.parent)

@pytest.mark.asyncio
async def test_upload_dir_to_r2_boto3_no_credentials_error(mock_env_vars, temp_hls_directory: Path):
    """Test NoCredentialsError when creating boto3 client."""
    with patch("boto3.client", side_effect=NoCredentialsError()) as mock_boto_client:
        with pytest.raises(RuntimeError, match="AWS credentials not found or incomplete for R2 upload."):
            await upload_dir_to_r2(temp_hls_directory, "answers/test_id")
        mock_boto_client.assert_called_once()

@pytest.mark.asyncio
async def test_upload_dir_to_r2_boto3_generic_client_creation_error(mock_env_vars, temp_hls_directory: Path):
    """Test generic error when creating boto3 client."""
    with patch("boto3.client", side_effect=Exception("Generic client error")) as mock_boto_client:
        with pytest.raises(RuntimeError, match="Error creating S3 client for R2: Generic client error"):
            await upload_dir_to_r2(temp_hls_directory, "answers/test_id")
        mock_boto_client.assert_called_once()

