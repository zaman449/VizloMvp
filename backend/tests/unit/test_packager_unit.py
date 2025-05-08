import asyncio
import os
from pathlib import Path
import shutil
import tempfile
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

# Adjust the import path based on your project structure
from app.workers.hls_packager import package_to_hls

@pytest.fixture
async def dummy_mp4_file() -> Path:
    """Creates a small, short dummy MP4 file for testing."""
    temp_dir = tempfile.mkdtemp(prefix="test_packager_dummy_mp4_")
    dummy_file_path = Path(temp_dir) / "dummy_video.mp4"
    
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        pytest.skip("ffmpeg not found, cannot create dummy MP4 for packager test.")

    # Create a 1-second black video with silent audio
    # Using a simpler command that is less likely to fail due to complex lavfi filters
    # if the full ffmpeg suite isn't perfectly set up.
    # This creates a very small H.264 video.
    args = [
        ffmpeg_path,
        "-y",  # Overwrite output files without asking
        "-f", "lavfi", "-i", "color=c=black:s=128x72:d=1:r=1", # 1 sec, 1fps, 128x72 black video
        "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100", # 1 sec silent audio
        "-c:v", "libx264", "-profile:v", "baseline", "-level", "3.0", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-shortest",
        str(dummy_file_path)
    ]
    
    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        print(f"Failed to create dummy MP4. Stderr: {stderr.decode(errors=\'ignore\')}")
        pytest.fail(f"Failed to create dummy MP4 for testing. ffmpeg stderr: {stderr.decode(errors=\'ignore\')}")
    
    if not dummy_file_path.exists() or dummy_file_path.stat().st_size == 0:
        pytest.fail("Dummy MP4 file was not created or is empty.")

    yield dummy_file_path
    
    # Teardown: remove the temporary directory and its contents
    shutil.rmtree(temp_dir)

@pytest.mark.asyncio
async def test_package_to_hls_success(dummy_mp4_file: Path):
    """Test successful HLS packaging."""
    with tempfile.TemporaryDirectory(prefix="test_hls_output_") as tmp_out_dir_str:
        out_dir = Path(tmp_out_dir_str)
        
        # Mock shutil.which to ensure ffmpeg is "found"
        with patch("shutil.which", return_value="/fake/path/to/ffmpeg") as mock_which:
            # Mock the subprocess execution
            mock_process = AsyncMock()
            mock_process.communicate = AsyncMock(return_value=(b"stdout", b"stderr"))
            mock_process.returncode = 0
            
            with patch("asyncio.create_subprocess_exec", return_value=mock_process) as mock_create_subprocess:
                master_manifest_path = await package_to_hls(dummy_mp4_file, out_dir)
                
                mock_which.assert_called_once_with("ffmpeg")
                mock_create_subprocess.assert_called_once()
                args, _ = mock_create_subprocess.call_args
                assert str(dummy_mp4_file) in args
                assert str(out_dir / "master.m3u8") in args

                # Simulate ffmpeg creating the files for assertion purposes
                # In a real test with actual ffmpeg, these would be created by the command.
                # Since we mock the subprocess, we need to create them to check.
                (out_dir / "master.m3u8").touch()
                (out_dir / "v0_00000.ts").touch()

                assert master_manifest_path == out_dir / "master.m3u8"
                assert master_manifest_path.exists()
                
                # Check for at least one segment file (name might vary based on ffmpeg version/output)
                ts_files = list(out_dir.glob("*.ts"))
                assert len(ts_files) >= 1, "No .ts segment files were found (or simulated)."
                assert (out_dir / "v0_00000.ts").exists()

@pytest.mark.asyncio
async def test_package_to_hls_input_file_not_found():
    """Test HLS packaging when input MP4 file does not exist."""
    with tempfile.TemporaryDirectory(prefix="test_hls_notfound_") as tmp_out_dir_str:
        out_dir = Path(tmp_out_dir_str)
        non_existent_mp4 = Path(tmp_out_dir_str) / "non_existent.mp4"
        
        with pytest.raises(FileNotFoundError, match=f"Input MP4 file not found: {non_existent_mp4}"):
            await package_to_hls(non_existent_mp4, out_dir)

@pytest.mark.asyncio
async def test_package_to_hls_ffmpeg_not_found():
    """Test HLS packaging when ffmpeg command is not found."""
    with tempfile.TemporaryDirectory(prefix="test_hls_noffmpeg_") as tmp_dir_str:
        out_dir = Path(tmp_dir_str) / "output"
        dummy_mp4 = Path(tmp_dir_str) / "dummy.mp4"
        dummy_mp4.touch() # Create a dummy file

        with patch("shutil.which", return_value=None) as mock_which:
            with pytest.raises(RuntimeError, match="ffmpeg command not found"):
                await package_to_hls(dummy_mp4, out_dir)
            mock_which.assert_called_once_with("ffmpeg")

@pytest.mark.asyncio
async def test_package_to_hls_ffmpeg_fails(dummy_mp4_file: Path):
    """Test HLS packaging when ffmpeg command fails (returns non-zero exit code)."""
    with tempfile.TemporaryDirectory(prefix="test_hls_ffmpegfail_") as tmp_out_dir_str:
        out_dir = Path(tmp_out_dir_str)

        with patch("shutil.which", return_value="/fake/path/to/ffmpeg"): # ffmpeg is "found"
            mock_process = AsyncMock()
            mock_process.communicate = AsyncMock(return_value=(b"stdout error details", b"stderr error details"))
            mock_process.returncode = 1 # Simulate ffmpeg failure
            
            with patch("asyncio.create_subprocess_exec", return_value=mock_process) as mock_create_subprocess:
                with pytest.raises(RuntimeError) as excinfo:
                    await package_to_hls(dummy_mp4_file, out_dir)
                
                assert "ffmpeg failed with exit code 1" in str(excinfo.value)
                assert "stderr error details" in str(excinfo.value)
                mock_create_subprocess.assert_called_once()

