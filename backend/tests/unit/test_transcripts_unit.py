import asyncio, pytest
from pathlib import Path
from types import SimpleNamespace
from app.workers import transcripts  # backend/app/workers/transcripts.py

# ---------- fixtures & helpers ----------

@pytest.fixture
def monkeypatched_encoder(monkeypatch):
    """
    Make token counts deterministic: 1 token per word.
    """
    monkeypatch.setattr(
        transcripts,
        "ENCODER",
        SimpleNamespace(
            encode=lambda s: s.split(),          # returns list of "tokens"
            decode=lambda t: " ".join(t),
        ),
    )

# ---------- tests ----------

def test_chunk_empty(monkeypatched_encoder):
    assert transcripts.chunk("") == []

def test_chunk_single_chunk(monkeypatched_encoder):
    txt = "word " * 50  # 50 tokens < max_tokens=400
    chunks = transcripts.chunk(txt)
    assert len(chunks) == 1
    assert chunks[0].text.strip() == txt.strip()

def test_chunk_overlap(monkeypatched_encoder):
    txt = "word " * 850    # 850 tokens
    chunks = transcripts.chunk(txt, max_tokens=400, overlap=50)
    # expected: ceil((850	400)/(400	50)) + 1  == 3
    assert len(chunks) == 3
    # overlap check: second chunk starts at 350th token
    assert chunks[1].start_sec == int(350/3.2)

@pytest.mark.asyncio
async def test_fetch_captions_success(monkeypatch):
    def fake_get(video_id, languages):
        return [{"text": "hello"}, {"text": "world"}]
    monkeypatch.setattr(
        transcripts.YouTubeTranscriptApi,
        "get_transcript",
        fake_get,
    )
    result = await transcripts.fetch_captions("abc123")
    assert result == "hello world"

@pytest.mark.asyncio
async def test_fetch_captions_no_transcript(monkeypatch):
    from youtube_transcript_api._errors import NoTranscriptFound
    def fake_get(video_id, languages):
        # For NoTranscriptFound, the constructor expects:
        # video_id: str, requested_language_codes: Iterable[str], transcript_data: "TranscriptList"
        # We can use dummy values for the test.
        raise NoTranscriptFound(video_id=video_id, requested_language_codes=languages, transcript_data=[])
    monkeypatch.setattr(
        transcripts.YouTubeTranscriptApi,
        "get_transcript",
        fake_get,
    )
    result = await transcripts.fetch_captions("def456")
    assert result == ""  # empty string triggers skip logic
