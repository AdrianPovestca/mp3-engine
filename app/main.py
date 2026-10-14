
from pathlib import Path
from urllib.parse import urlparse
import ipaddress
import socket
import subprocess
import uuid

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, HttpUrl
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

BASE_DIR = Path(__file__).resolve().parent.parent
APP_DIR = BASE_DIR / "app"
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILE_SIZE = 50 * 1024 * 1024
ALLOWED_AUDIO_EXTENSIONS = {
    ".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac", ".opus"
}
YOUTUBE_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "music.youtube.com", "youtu.be"
}

app = FastAPI(title="MP3 Downloader", version="2.0.0")


class URLRequest(BaseModel):
    url: HttpUrl


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def homepage():
    index_file = APP_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(500, "app/index.html is missing.")
    return index_file.read_text(encoding="utf-8")


@app.get("/api/search")
async def search(q: str, limit: int = 10):
    q = q.strip()
    if not q:
        raise HTTPException(400, "Enter a song or artist.")

    limit = max(1, min(limit, 10))

    options = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
        "skip_download": True,
        "playlistend": limit,
        "noplaylist": True,
    }

    try:
        def run_search():
            with yt_dlp.YoutubeDL(options) as ydl:
                return ydl.extract_info(
                    f"ytsearch{limit}:{q}",
                    download=False,
                )

        data = await run_in_threadpool(run_search)
        results = []

        for item in data.get("entries", []):
            if not item:
                continue

            video_id = item.get("id")
            source_url = (
                f"https://www.youtube.com/watch?v={video_id}"
                if video_id
                else item.get("url")
            )

            results.append({
                "title": item.get("title") or "Untitled",
                "artist": item.get("channel") or item.get("uploader") or "",
                "duration": item.get("duration"),
                "thumbnail": item.get("thumbnail"),
                "source_url": source_url,
            })

        return {"query": q, "results": results}

    except Exception as exc:
        raise HTTPException(
            502, f"Search failed: {str(exc)[:200]}"
        ) from exc


@app.post("/api/youtube-download")
async def download_youtube(request: URLRequest):
    url = str(request.url)
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()

    if parsed.scheme != "https" or host not in YOUTUBE_HOSTS:
        raise HTTPException(400, "Paste a valid HTTPS YouTube link.")

    job_id = uuid.uuid4().hex
    output_template = str(DOWNLOAD_DIR / f"{job_id}.%(ext)s")
    output_path = DOWNLOAD_DIR / f"{job_id}.mp3"

    options = {
        "format": "bestaudio/best",
        "outtmpl": output_template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 20,
        "retries": 2,
        "max_filesize": MAX_FILE_SIZE,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }],
    }

    try:
        def download_audio():
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=True)
                return info.get("title") or "youtube-audio"

        title = await run_in_threadpool(download_audio)

        if not output_path.exists() or output_path.stat().st_size == 0:
            raise RuntimeError("MP3 file was not created.")

        safe_title = "".join(
            c for c in title
            if c.isalnum() or c in " -_()"
        ).strip()[:100]

        filename = (safe_title or "youtube-audio") + ".mp3"

        return FileResponse(
            path=output_path,
            media_type="audio/mpeg",
            filename=filename,
            background=BackgroundTask(
                lambda: output_path.unlink(missing_ok=True)
            ),
        )

    except Exception as exc:
        for path in DOWNLOAD_DIR.glob(f"{job_id}.*"):
            path.unlink(missing_ok=True)

        if isinstance(exc, HTTPException):
            raise

        raise HTTPException(
            502, f"YouTube conversion failed: {str(exc)[:200]}"
        ) from exc


@app.post("/api/download")
async def download_direct(request: URLRequest):
    url = str(request.url)
    parsed = urlparse(url)

    if parsed.scheme != "https" or not parsed.hostname:
        raise HTTPException(400, "Use a valid HTTPS audio URL.")

    extension = Path(parsed.path).suffix.lower()
    if extension not in ALLOWED_AUDIO_EXTENSIONS:
        raise HTTPException(
            400,
            "Use a direct audio URL ending in MP3, WAV, M4A, AAC, "
            "OGG, FLAC or OPUS. For YouTube links, use the YouTube form."
        )

    # Avoid requests to local/private IP addresses.
    try:
        addresses = await run_in_threadpool(
            socket.getaddrinfo, parsed.hostname, None
        )
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if not ip.is_global:
                raise HTTPException(400, "This audio host is not allowed.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, "Could not verify the audio host.") from exc

    job_id = uuid.uuid4().hex
    source_path = DOWNLOAD_DIR / f"{job_id}{extension}"
    output_path = DOWNLOAD_DIR / f"{job_id}.mp3"

    try:
        size = 0

        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=45,
        ) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                content_type = response.headers.get(
                    "content-type", ""
                ).lower()

                if not (
                    content_type.startswith("audio/")
                    or "application/octet-stream" in content_type
                    or "binary/octet-stream" in content_type
                ):
                    raise HTTPException(
                        400, "The URL did not return an audio file."
                    )

                with source_path.open("wb") as file:
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_FILE_SIZE:
                            raise HTTPException(
                                413, "Maximum file size is 50 MB."
                            )
                        file.write(chunk)

        if size == 0:
            raise HTTPException(400, "The audio file is empty.")

        await run_in_threadpool(convert_to_mp3, source_path, output_path)

        return FileResponse(
            path=output_path,
            media_type="audio/mpeg",
            filename="audio.mp3",
            background=BackgroundTask(
                lambda: output_path.unlink(missing_ok=True)
            ),
        )

    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            502, f"Audio source returned HTTP {exc.response.status_code}."
        ) from exc
    except Exception as exc:
        raise HTTPException(
            502, f"Download or conversion failed: {str(exc)[:200]}"
        ) from exc
    finally:
        source_path.unlink(missing_ok=True)


@app.post("/api/convert-upload")
async def convert_upload(file: UploadFile = File(...)):
    extension = Path(file.filename or "").suffix.lower()

    if extension not in ALLOWED_AUDIO_EXTENSIONS:
        raise HTTPException(
            400,
            "Upload MP3, WAV, M4A, AAC, OGG, FLAC or OPUS."
        )

    job_id = uuid.uuid4().hex
    source_path = DOWNLOAD_DIR / f"{job_id}{extension}"
    output_path = DOWNLOAD_DIR / f"{job_id}.mp3"

    try:
        size = 0

        with source_path.open("wb") as destination:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break

                size += len(chunk)
                if size > MAX_FILE_SIZE:
                    raise HTTPException(413, "Maximum file size is 50 MB.")

                destination.write(chunk)

        if size == 0:
            raise HTTPException(400, "The uploaded file is empty.")

        await run_in_threadpool(convert_to_mp3, source_path, output_path)

        return FileResponse(
            path=output_path,
            media_type="audio/mpeg",
            filename="converted.mp3",
            background=BackgroundTask(
                lambda: output_path.unlink(missing_ok=True)
            ),
        )

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            500, f"Conversion failed: {str(exc)[:200]}"
        ) from exc
    finally:
        source_path.unlink(missing_ok=True)
        await file.close()


def convert_to_mp3(source: Path, destination: Path):
    result = subprocess.run(
        [
            "ffmpeg", "-y",
            "-i", str(source),
            "-vn",
            "-codec:a", "libmp3lame",
            "-b:a", "192k",
            str(destination),
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )

    if result.returncode != 0 or not destination.exists():
        details = (
            result.stderr[-400:]
            if result.stderr
            else "Unknown FFmpeg error."
        )
        raise RuntimeError(f"MP3 conversion failed: {details}")
