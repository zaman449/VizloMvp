
## 0.4.0 — 2025-05-08

- Added HLS packaging via ffmpeg (`hls_packager.py`)
- Implemented Cloudflare R2 uploader (`uploader.py`) with retries
- Extended `answers` table (`hls_manifest_url`, `status=LIVE`)
- New API: `POST /api/publish-video/{answer_id}`
- Front-end player (hls.js) + citation sidebar
- CI: moto S3 mock and Cloudflare env vars
- Dev script `dev_publish_answer.py`


