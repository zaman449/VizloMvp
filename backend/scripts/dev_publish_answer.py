import argparse
import asyncio
import logging
import os
import sys
from uuid import UUID

# Add the backend directory to sys.path to allow imports from app
# This assumes the script is in vizlo/backend/scripts/
# and the app module is in vizlo/backend/app/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.dirname(SCRIPT_DIR) # This should be /home/ubuntu/vizlo/backend
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

try:
    from app.services.publish_answer import publish_answer, AnswerStatus
except ImportError as e:
    print(f"Error: Could not import publish_answer service. Ensure PYTHONPATH is set correctly or run from project root.")
    print(f"Current sys.path: {sys.path}")
    print(f"Details: {e}")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def main():
    parser = argparse.ArgumentParser(description="Publish an answer video to HLS and R2.")
    parser.add_argument(
        "--answer_id",
        type=str,
        required=True,
        help="The UUID of the answer to publish."
    )
    args = parser.parse_args()

    try:
        answer_id = UUID(args.answer_id)
    except ValueError:
        logger.error(f"Invalid UUID format for answer_id: {args.answer_id}")
        return

    logger.info(f"Starting publishing process for answer_id: {answer_id}")

    # For the dev script, ensure necessary environment variables are at least acknowledged.
    # The uploader service itself checks for these.
    required_r2_vars = ["CF_R2_KEY", "CF_R2_SECRET", "CF_R2_ENDPOINT", "CF_R2_BUCKET", "CF_PUBLIC_CDN"]
    missing_vars = [var for var in required_r2_vars if not os.getenv(var)]
    if missing_vars:
        logger.warning(f"The following R2 environment variables are not set: {', '.join(missing_vars)}")
        logger.warning("The uploader service will fail if these are not configured.")
        # Optionally, exit or provide default dummy values for a pure dev script run if appropriate
        # For now, let the service handle the error if they are truly needed and missing.

    # The publish_answer service uses placeholder DB functions.
    # Ensure a dummy MP4 exists as per the service logic for get_answer_by_id placeholder.
    # e.g., /media/answers/<answer_id>.mp4
    # This script assumes that such a file is already in place from a previous step (e.g., Slice 3 output).
    logger.info(f"Ensure that the MP4 file for answer {answer_id} exists at the expected location (e.g., /media/answers/{answer_id}.mp4) for the placeholder service logic to work.")

    try:
        result = await publish_answer(answer_id)

        if result.get("status") == AnswerStatus.LIVE:
            logger.info(f"Successfully published answer {answer_id}.")
            print(f"Public HLS URL: {result.get('url')}")
        else:
            logger.error(f"Failed to publish answer {answer_id}.")
            logger.error(f"Status: {result.get('status')}")
            logger.error(f"Message: {result.get('message', 'No additional message.')}")
            if result.get("code"):
                logger.error(f"Error Code: {result.get('code')}")

    except Exception as e:
        logger.error(f"An unexpected error occurred while running the publish script for {answer_id}: {e}", exc_info=True)

if __name__ == "__main__":
    # Example of how to run for local dev if env vars are in a .env file
    # from dotenv import load_dotenv
    # DOTENV_PATH = os.path.join(BACKEND_DIR, ".env") # Assuming .env is in backend directory
    # if os.path.exists(DOTENV_PATH):
    #     load_dotenv(DOTENV_PATH)
    #     logger.info(f"Loaded environment variables from {DOTENV_PATH}")
    # else:
    #     logger.info(f".env file not found at {DOTENV_PATH}, relying on system environment variables.")

    asyncio.run(main())

