import asyncio
import logging
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

async def package_to_hls(mp4_path: Path, out_dir: Path) -> Path:
    """
    Transcodes an MP4 file to HLS format (master.m3u8 and .ts segments).

    Args:
        mp4_path: Path to the input MP4 file.
        out_dir: Directory where HLS files will be saved.

    Returns:
        Path to the master HLS manifest file (master.m3u8).

    Raises:
        FileNotFoundError: If mp4_path does not exist.
        RuntimeError: If ffmpeg command fails.
    """
    if not mp4_path.exists():
        raise FileNotFoundError(f"Input MP4 file not found: {mp4_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    master_manifest_path = out_dir / "master.m3u8"
    segment_filename_template = out_dir / "v0_%05d.ts"

    # Check if ffmpeg is available
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg command not found. Please ensure it is installed and in PATH.")

    # ffmpeg command arguments
    # -i: input file
    # -c:v libx264: video codec H.264
    # -vf scale=-2:720: scale to 720p height, maintain aspect ratio
    # -b:v 3M: target video bitrate 3 Mbps (this is average, maxrate could be used for stricter control)
    # -maxrate 3M -bufsize 6M: (Alternative) for stricter max bitrate control
    # -c:a aac: audio codec AAC
    # -b:a 128k: audio bitrate 128 kbps
    # -f hls: output format HLS
    # -hls_time 10: segment duration 10 seconds
    # -hls_playlist_type vod: playlist type Video on Demand
    # -hls_segment_filename: template for segment filenames
    # -start_number 0: start segment numbering from 0
    # master_manifest_path: output master manifest file
    # -y: overwrite output files without asking
    args = [
        ffmpeg_path,
        "-i", str(mp4_path),
        "-c:v", "libx264",
        "-vf", "scale=-2:720",
        "-b:v", "3M", # Target average bitrate
        # "-maxrate", "3M", "-bufsize", "6M", # Stricter max bitrate control if needed
        "-c:a", "aac",
        "-b:a", "128k",
        "-f", "hls",
        "-hls_time", "10",
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", str(segment_filename_template),
        "-start_number", "0",
        str(master_manifest_path),
        "-y"
    ]

    logger.info(f"Starting HLS packaging for {mp4_path} to {out_dir}")
    logger.debug(f"ffmpeg command: {" ".join(args)}")

    process = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        error_message = f"ffmpeg failed with exit code {process.returncode}.\n"
        if stdout:
            error_message += f"Stdout:\n{stdout.decode(errors='ignore')}\n"
        if stderr:
            error_message += f"Stderr:\n{stderr.decode(errors='ignore')}"
        logger.error(error_message)
        raise RuntimeError(error_message)

    logger.info(f"HLS packaging successful for {mp4_path}. Master manifest: {master_manifest_path}")
    return master_manifest_path

# Example usage (for testing purposes, not part of the worker itself)
# async def main():
#     # Create a dummy mp4 file for testing
#     dummy_mp4 = Path("dummy_video.mp4")
#     if not dummy_mp4.exists():
#         # This command creates a 1-second black video with silent audio
#         # Ensure ffmpeg is installed to run this part
#         try:
#             ff_create_dummy = await asyncio.create_subprocess_exec(
#                 shutil.which("ffmpeg"), "-y", "-f", "lavfi", "-i", "color=c=black:s=1280x720:d=1", 
#                 "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100", 
#                 "-c:v", "libx264", "-c:a", "aac", "-shortest", str(dummy_mp4)
#             )
#             await ff_create_dummy.communicate()
#             if ff_create_dummy.returncode != 0:
#                 print("Failed to create dummy MP4 for testing.")
#                 return
#         except Exception as e:
#             print(f"Error creating dummy MP4: {e}")
#             return

#     output_directory = Path("hls_output")
#     try:
#         manifest_file = await package_to_hls(dummy_mp4, output_directory)
#         print(f"HLS packaging complete. Manifest: {manifest_file}")
#         # Clean up dummy files
#         # if dummy_mp4.exists():
#         #     dummy_mp4.unlink()
#         # if output_directory.exists():
#         #     shutil.rmtree(output_directory)
#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())

