import asyncio
import os
from pathlib import Path
import shutil
import tempfile
import uuid
import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from fastapi.testclient import TestClient

# Assuming your FastAPI app and specific routers/services are structured like this:
# This will need adjustment based on the actual project structure and how the FastAPI app is created.
# For now, let's assume a main.py or similar that creates the app.
# from app.main import app # Placeholder for your FastAPI app instance
# from app.services.publish_answer import AnswerStatus # Assuming this is accessible
# from app.db.models import Answer # If using actual DB models for setup

# --- Mocking FastAPI app for testing --- #
# If app.main.app is not directly importable or setup is complex, 
# we might need to create a minimal app instance for testing here.
# For this example, we will assume `app` can be imported or we mock its routes.

# Create a dummy FastAPI app for the TestClient if not easily importable
from fastapi import FastAPI
from app.api.routes.answers_api import router as answers_router # Assuming this path

@pytest.fixture(scope="module")
def test_app():
    app = FastAPI()
    app.include_router(answers_router, prefix="/api")
    # Initialize dummy data for placeholders in answers_api.py
    from app.api.routes import answers_api
    answers_api._db_answers = {} # Clear at start of module tests
    answers_api._db_citations = {} # Clear at start of module tests
    return app

@pytest.fixture
def client(test_app):
    with TestClient(test_app) as c:
        yield c

@pytest.fixture
def mock_r2_env_vars(monkeypatch):
    monkeypatch.setenv("CF_R2_KEY", "test_integration_r2_key")
    monkeypatch.setenv("CF_R2_SECRET", "test_integration_r2_secret")
    monkeypatch.setenv("CF_R2_ENDPOINT", "https://int-test.r2.endpoint.com")
    monkeypatch.setenv("CF_R2_BUCKET", "int-test-bucket")
    monkeypatch.setenv("CF_PUBLIC_CDN", "https://int-test-cdn.com")

@pytest.fixture
def dummy_answer_setup(tmp_path: Path) -> dict:
    answer_id = uuid.uuid4()
    slug = f"test-answer-{answer_id}"
    mp4_dir = tmp_path / "media" / "answers"
    mp4_dir.mkdir(parents=True, exist_ok=True)
    mp4_path = mp4_dir / f"{answer_id}.mp4"
    
    # Create a small dummy MP4 file
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        pytest.skip("ffmpeg not found, cannot create dummy MP4 for integration test.")
    
    args = [
        ffmpeg_path, "-y",
        "-f", "lavfi", "-i", "color=c=blue:s=128x72:d=1:r=1",
        "-f", "lavfi", "-i", "anullsrc=channel_layout=mono:sample_rate=22050",
        "-c:v", "libx264", "-profile:v", "baseline", "-level", "1.0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest",
        str(mp4_path)
    ]
    # Run ffmpeg synchronously for fixture setup
    import subprocess
    process = subprocess.run(args, capture_output=True, text=True)
    if process.returncode != 0:
        pytest.fail(f"Failed to create dummy MP4 for integration test. ffmpeg stderr: {process.stderr}")
    
    if not mp4_path.exists() or mp4_path.stat().st_size == 0:
        pytest.fail("Dummy MP4 for integration test was not created or is empty.")

    # Initial answer data for the placeholder DB in answers_api.py
    initial_answer_data = {
        "id": answer_id,
        "slug": slug,
        "title": "Integration Test Answer",
        "status": "READY", # Must be READY to be published
        "mp4_path": str(mp4_path), # Path for the publish_answer service
        "hls_manifest_url": None,
        "video_url": None,
        "citations": [
            {"video_id": "yt123", "start_sec": 10, "text": "Sample citation 1"}
        ],
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    return initial_answer_data

@pytest.mark.asyncio
async def test_publish_flow_e2e(
    client: TestClient, 
    dummy_answer_setup: dict, 
    mock_r2_env_vars,
    tmp_path: Path # For temporary HLS output from packager
):
    """Integration test for the video publishing flow."""
    answer_id = dummy_answer_setup["id"]
    slug = dummy_answer_setup["slug"]
    mp4_path_str = dummy_answer_setup["mp4_path"]

    # --- Setup: Mock external services and DB --- #
    # 1. Patch the placeholder DB in answers_api to include our test answer
    from app.api.routes import answers_api
    answers_api._db_answers[answer_id] = dummy_answer_setup
    answers_api._db_citations[answer_id] = dummy_answer_setup["citations"]

    # 2. Mock the actual HLS packaging and R2 upload to avoid external calls
    #    and to control their output for the test.
    mock_hls_packager = AsyncMock(name="mock_package_to_hls")
    # The packager should create files in a temp dir and return the master manifest path
    # We need to simulate this behavior.
    async def fake_package_to_hls(mp4_p: Path, out_d: Path) -> Path:
        # Simulate HLS files being created
        master_m3u8 = out_d / "master.m3u8"
        master_m3u8.write_text("#EXTM3U...")
        (out_d / "v0_00000.ts").write_text("segment data")
        return master_m3u8
    mock_hls_packager.side_effect = fake_package_to_hls

    mock_r2_uploader = AsyncMock(name="mock_upload_dir_to_r2")
    expected_public_url = f"https://int-test-cdn.com/answers/{answer_id}/master.m3u8"
    mock_r2_uploader.return_value = expected_public_url

    # The publish_answer service uses these placeholder DB functions.
    # We need to ensure they reflect changes or mock them if they are too simple.
    # For this test, we will rely on the _db_answers in answers_api being updated by the service logic
    # via the placeholder update_answer_status_and_url.
    async def fake_get_answer_by_id(ans_id):
        return answers_api._db_answers.get(ans_id)

    async def fake_update_answer(ans_id, status, hls_url=None, video_url=None):
        if ans_id in answers_api._db_answers:
            answers_api._db_answers[ans_id]["status"] = status
            if hls_url: answers_api._db_answers[ans_id]["hls_manifest_url"] = hls_url
            if video_url: answers_api._db_answers[ans_id]["video_url"] = video_url
            answers_api._db_answers[ans_id]["updated_at"] = "now"
            return True
        return False

    with patch("app.services.publish_answer.package_to_hls", mock_hls_packager),
         patch("app.services.publish_answer.upload_dir_to_r2", mock_r2_uploader),
         patch("app.services.publish_answer.get_answer_by_id", side_effect=fake_get_answer_by_id) as mock_service_get_answer,
         patch("app.services.publish_answer.update_answer_status_and_url", side_effect=fake_update_answer) as mock_service_update_answer,
         patch("tempfile.mkdtemp", return_value=str(tmp_path / "test_hls_temp_output")) as mock_mkdtemp: # Control temp dir
        
        # --- Act: Call the publish API endpoint --- #
        # The API currently calls publish_answer directly, not as a background task.
        # So, we expect a 200 if successful, or an error code.
        response = client.post(f"/api/publish-video/{answer_id}")

        # --- Assert: Initial publish call --- #
        assert response.status_code == 200, f"Publish API call failed: {response.text}"
        response_json = response.json()
        assert response_json["status"] == "LIVE"
        assert response_json["url"] == expected_public_url

        # Verify mocks were called
        mock_service_get_answer.assert_called_with(answer_id)
        mock_hls_packager.assert_called_once()
        # Check args of hls_packager: mp4_path and an output directory within the controlled temp_path
        assert mock_hls_packager.call_args[0][0] == Path(mp4_path_str)
        assert mock_hls_packager.call_args[0][1].parent == tmp_path # out_dir is inside tmp_path/test_hls_temp_output
        
        mock_r2_uploader.assert_called_once()
        # Check args of r2_uploader: the output dir from packager and remote_prefix
        assert mock_r2_uploader.call_args[0][0] == mock_hls_packager.call_args[0][1]
        assert mock_r2_uploader.call_args[0][1] == f"answers/{answer_id}"

        mock_service_update_answer.assert_called_with(answer_id, "LIVE", hls_url=expected_public_url, video_url=expected_public_url)
        mock_mkdtemp.assert_called_once() # Ensure temp dir was created

        # --- Assert: Polling for answer status (simulated by direct GET) --- #
        # The publish call is synchronous in the current API, so status should be LIVE immediately.
        # If it were truly async with polling, the loop would be here.
        
        poll_response = client.get(f"/api/answer/{slug}")
        assert poll_response.status_code == 200, f"Polling answer failed: {poll_response.text}"
        polled_answer_data = poll_response.json()

        assert polled_answer_data["status"] == "LIVE"
        assert polled_answer_data["hls_manifest_url"] == expected_public_url
        assert polled_answer_data["video_url"] == expected_public_url # As per spec
        assert polled_answer_data["id"] == str(answer_id)
        assert len(polled_answer_data["citations"]) == 1
        assert polled_answer_data["citations"][0]["video_id"] == "yt123"

        # --- Cleanup (handled by fixtures and tmp_path) --- #
        # Ensure the placeholder DB is cleared for other tests if necessary (though fixture scope helps)
        del answers_api._db_answers[answer_id]
        if answer_id in answers_api._db_citations:
            del answers_api._db_citations[answer_id]

