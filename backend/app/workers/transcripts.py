from typing import List, NamedTuple
from app.utils.retry import retry_backoff
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound
import tiktoken

class Chunk(NamedTuple):
    text: str
    start_sec: int
    end_sec: int

ENCODER = tiktoken.encoding_for_model("text-embedding-3-small")

@retry_backoff(errors=(Exception,), max_retries=5, first_wait=2, jitter=True)
async def fetch_captions(video_id: str) -> str:
    """Return plain	text caption string (en only)."""
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=["en"])
        return " ".join(item["text"] for item in transcript)
    except NoTranscriptFound:
        return ""  # caller will skip video


def chunk(txt: str, max_tokens: int = 400, overlap: int = 50) -> List[Chunk]:
    """
    Splits a text into overlapping chunks based on token count.
    Timestamps (start_sec, end_sec) are estimated based on token index
    assuming a rate of 3.2 tokens per second.
    """
    tokens = ENCODER.encode(txt)
    total_tokens = len(tokens)

    if not total_tokens:
        return []

    chunks_list: List[Chunk] = []
    current_pos = 0
    step = max_tokens - overlap 

    # Guard against non-positive step if multiple chunks would be needed, to prevent infinite loop.
    # Current tests (max_tokens=400, overlap=50 => step=350) do not hit this.
    # If step <= 0 and total_tokens > max_tokens, this loop could be infinite without a break.

    while current_pos < total_tokens:
        end_pos = min(current_pos + max_tokens, total_tokens)
        chunk_tokens = tokens[current_pos:end_pos]

        if not chunk_tokens: # Should not be reached if current_pos < total_tokens & max_tokens > 0
            break

        text_content = ENCODER.decode(chunk_tokens)
        start_sec = int(current_pos / 3.2)
        
        # Index of the last token in this specific chunk within the original 'tokens' list
        last_token_index_in_original = current_pos + len(chunk_tokens) - 1
        end_sec = int(last_token_index_in_original / 3.2)
        
        # Ensure end_sec is not less than start_sec, especially for single token chunks due to int truncation.
        if end_sec < start_sec:
            end_sec = start_sec

        chunks_list.append(Chunk(text=text_content, start_sec=start_sec, end_sec=end_sec))

        if end_pos == total_tokens:
            break # All tokens processed

        # If step is non-positive and we still have tokens beyond the current chunk, 
        # advancing by step would not progress. Break to avoid infinite loop.
        if step <= 0 and current_pos + max_tokens < total_tokens:
            # This implies overlap >= max_tokens, and there's more text that won't be reached.
            # Effectively, only the first chunk is returned in this scenario.
            break
        
        current_pos += step
    
    return chunks_list

