# Documentation for Transcript Processing Functions

This document outlines the behavior and usage of the `fetch_captions` and `chunk` functions found in `backend/app/workers/transcripts.py`.

## `fetch_captions(video_id: str) -> str`

**Purpose:**
This asynchronous function retrieves the English (en) plain-text transcript for a given YouTube video ID.

**Parameters:**
- `video_id` (str): The unique identifier of the YouTube video (e.g., "dQw4w9WgXcQ"). This should be the ID itself, not the full URL.

**Returns:**
- `str`: A single string containing the concatenated text of the English transcript. Each transcript item's text is joined by a space.
- If no English transcript is available for the video, or if the `youtube_transcript_api` raises a `NoTranscriptFound` error (or other specified exceptions), the function returns an empty string (`""`). This behavior is intended to allow the calling process to skip videos without transcripts.

**Behavior and Error Handling:**
- The function specifically requests English transcripts (`languages=["en"]`).
- It utilizes a `retry_backoff` decorator. This means that if the underlying `YouTubeTranscriptApi.get_transcript` call fails with any `Exception` (as configured in the decorator), the function will attempt to retry the call up to 5 times. The first retry will occur after a 2-second wait, with subsequent waits increasing exponentially. Jitter is also applied to the wait times to prevent thundering herd problems.
- If, after all retries, a transcript cannot be fetched (e.g., due to `NoTranscriptFound`), an empty string is returned.

**Dependencies:**
- `youtube_transcript_api`: Used to fetch the transcript from YouTube.
- `app.utils.retry.retry_backoff`: Decorator for implementing retry logic.

**Unit Test Coverage:**
- `test_fetch_captions_success`: Mocks `YouTubeTranscriptApi.get_transcript` to return a sample transcript and verifies that the concatenated text is correctly returned.
- `test_fetch_captions_no_transcript`: Mocks `YouTubeTranscriptApi.get_transcript` to raise `NoTranscriptFound` and verifies that an empty string is returned.

## `chunk(txt: str, max_tokens: int = 400, overlap: int = 50) -> List[Chunk]`

**Purpose:**
This function splits a given text string into smaller, potentially overlapping chunks. Each chunk is designed to be under a specified maximum token count. The function also estimates start and end times (in seconds) for each chunk based on an assumed token rate.

**Parameters:**
- `txt` (str): The input text string to be chunked.
- `max_tokens` (int, optional): The maximum number of tokens allowed in a single chunk. Defaults to 400.
- `overlap` (int, optional): The number of tokens from the end of one chunk that should also be included at the beginning of the next chunk. Defaults to 50.

**Returns:**
- `List[Chunk]`: A list of `Chunk` named tuples. Each `Chunk` object has the following attributes:
    - `text` (str): The text content of the chunk.
    - `start_sec` (int): The estimated start time of the chunk in seconds from the beginning of the original text.
    - `end_sec` (int): The estimated end time of the chunk in seconds from the beginning of the original text.
- If the input text is empty (or results in zero tokens), an empty list (`[]`) is returned.

**Behavior and Logic:**
- **Tokenization:** The input text is first tokenized using `tiktoken.encoding_for_model("text-embedding-3-small")`. The tests use a monkeypatched encoder where each word is treated as one token for deterministic testing.
- **Chunking Process:**
    - The function iterates through the tokens, creating chunks up to `max_tokens` in length.
    - The `step` for moving to the start of the next chunk is `max_tokens - overlap`.
    - If the `step` is less than or equal to zero (i.e., `overlap >= max_tokens`) and there are still tokens remaining beyond the current chunk, the loop will break after processing the first chunk to prevent an infinite loop. In such a scenario, only the first chunk covering `max_tokens` is returned.
- **Timestamp Estimation:**
    - Start and end seconds for each chunk are estimated based on the token indices.
    - The estimation assumes a constant rate of **3.2 tokens per second**. This is a heuristic and may not perfectly align with actual speech or content timing.
    - `start_sec` is calculated as `int(current_token_position / 3.2)`.
    - `end_sec` is calculated as `int(last_token_index_in_chunk / 3.2)`.
    - A safeguard ensures that `end_sec` is not less than `start_sec`, especially for very short chunks where integer truncation might cause this.
- **Empty Input:** If the input `txt` is empty or results in no tokens, an empty list of chunks is returned immediately.
- **Single Chunk:** If the total number of tokens in `txt` is less than or equal to `max_tokens`, a single chunk containing the entire text is returned.

**Dependencies:**
- `tiktoken`: Used for tokenizing the input text.

**Unit Test Coverage:**
- `test_chunk_empty`: Verifies that an empty input string results in an empty list of chunks.
- `test_chunk_single_chunk`: Verifies that text shorter than `max_tokens` results in a single chunk containing the original text.
- `test_chunk_overlap`: Verifies correct chunking and overlap behavior for a text longer than `max_tokens`, including the number of chunks and the estimated `start_sec` of a subsequent chunk based on the overlap logic and the 3.2 tokens/sec assumption.

**Coverage Notes:**
- The current test suite achieves >90% branch coverage. The few missed lines (49, 60, 72 in `transcripts.py` at the time of writing) relate to specific edge conditions within the `chunk` function's looping and token handling logic (e.g., `if not chunk_tokens:` which is hard to hit if `max_tokens > 0`, and the `if step <= 0 and current_pos + max_tokens < total_tokens:` break condition which requires `overlap >= max_tokens`). These are considered minor edge cases not critical for the primary functionality given the typical usage parameters.
