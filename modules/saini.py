import os
import re
import time
import mmap
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
import tgcrypto  # noqa: F401
import subprocess
import concurrent.futures
from math import ceil  # noqa: F401
from utils import progress_bar
from pyrogram import Client, filters  # noqa: F401
from pyrogram.types import Message
from io import BytesIO  # noqa: F401
from pathlib import Path
from Crypto.Cipher import AES  # noqa: F401
from Crypto.Util.Padding import unpad  # noqa: F401
from base64 import b64decode  # noqa: F401

# Added imports for parallel + pipe helpers
import tempfile
import httpx
from typing import List, Tuple, Optional, Dict, Union
from urllib.parse import urlparse, unquote

# Global for retry logic in download_video
failed_counter = 0


def duration(filename):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries",
         "format=duration", "-of",
         "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    return float(result.stdout)


def exec(cmd):
    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.stdout.decode()
    print(output)
    return output
    # err = process.stdout.decode()


def pull_run(work, cmds):
    with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
        print("Waiting for tasks to complete")
        _ = list(executor.map(exec, cmds))


async def aio(url, name):
    k = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(k, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return k


async def download(url, name):
    ka = f'{name}.pdf'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(ka, mode='wb')
                await f.write(await resp.read())
                await f.close()
    return ka


def parse_vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = []
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ", 2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except Exception:
                pass
    return new_info


def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ", 3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.update({f'{i[2]}': f'{i[0]}'})
            except Exception:
                pass
    return new_info


async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        cmd1 = (
            f'yt-dlp -f "bv[height<={quality}]+ba/b" '
            f'-o "{output_path}/file.%(ext)s" '
            f'--allow-unplayable-format --no-check-certificate '
            f'--external-downloader aria2c "{mpd_url}"'
        )
        print(f"Running command: {cmd1}")
        os.system(cmd1)

        avDir = list(output_path.iterdir())
        print(f"Downloaded files: {avDir}")
        print("Decrypting")

        video_decrypted = False
        audio_decrypted = False

        for data in avDir:
            if data.suffix == ".mp4" and not video_decrypted:
                cmd2 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                print(f"Running command: {cmd2}")
                os.system(cmd2)
                if (output_path / "video.mp4").exists():
                    video_decrypted = True
                data.unlink()
            elif data.suffix == ".m4a" and not audio_decrypted:
                cmd3 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                print(f"Running command: {cmd3}")
                os.system(cmd3)
                if (output_path / "audio.m4a").exists():
                    audio_decrypted = True
                data.unlink()

        if not video_decrypted or not audio_decrypted:
            raise FileNotFoundError("Decryption failed: video or audio file not found.")

        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
        print(f"Running command: {cmd4}")
        os.system(cmd4)
        if (output_path / "video.mp4").exists():
            (output_path / "video.mp4").unlink()
        if (output_path / "audio.m4a").exists():
            (output_path / "audio.m4a").unlink()

        filename = output_path / f"{output_name}.mp4"

        if not filename.exists():
            raise FileNotFoundError("Merged video file not found.")

        cmd5 = f'ffmpeg -i "{filename}" 2>&1 | grep "Duration"'
        duration_info = os.popen(cmd5).read()
        print(f"Duration info: {duration_info}")

        return str(filename)

    except Exception as e:
        print(f"Error during decryption and merging: {str(e)}")
        raise


async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    print(f'[{cmd!r} exited with {proc.returncode}]')
    if proc.returncode == 1:
        return False
    if stdout:
        return f'[stdout]\n{stdout.decode()}'
    if stderr:
        return f'[stderr]\n{stderr.decode()}'


def old_download(url, file_name, chunk_size=1024 * 10):
    if os.path.exists(file_name):
        os.remove(file_name)
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(file_name, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                fd.write(chunk)
    return file_name


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def time_name():
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H%M%S")
    return f"{date} {current_time}.mp4"


async def download_video(url, cmd, name):
    download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader aria2c --downloader-args "aria2c: -x 16 -j 32"'
    global failed_counter
    print(download_cmd)
    logging.info(download_cmd)
    k = subprocess.run(download_cmd, shell=True)
    if "visionias" in cmd and k.returncode != 0 and failed_counter <= 10:
        failed_counter += 1
        await asyncio.sleep(5)
        await download_video(url, cmd, name)
    failed_counter = 0
    try:
        if os.path.isfile(name):
            return name
        elif os.path.isfile(f"{name}.webm"):
            return f"{name}.webm"
        name_root = name.split(".")[0]
        if os.path.isfile(f"{name_root}.mkv"):
            return f"{name_root}.mkv"
        elif os.path.isfile(f"{name_root}.mp4"):
            return f"{name_root}.mp4"
        elif os.path.isfile(f"{name_root}.mp4.webm"):
            return f"{name_root}.mp4.webm"

        return name
    except Exception:
        root, _ = os.path.splitext(name)
        return f"{root}.mp4"


# ============ NEW HELPERS: Parallel Download + HTTP Pipe + TG Upload ============

def is_url(s: str) -> bool:
    return isinstance(s, str) and s.startswith(("http://", "https://"))


def filename_from_url(url: str, fallback: str = "download.bin") -> str:
    try:
        p = urlparse(url)
        name = os.path.basename(p.path) or fallback
        return unquote(name)
    except Exception:
        return fallback


async def parallel_download(
    url_name_pairs: List[Tuple[str, str]],
    out_dir: str = ".",
    concurrency: int = 8,
    chunk_size: int = 131_072,
    timeout: Optional[int] = None
) -> List[str]:
    """
    Download multiple URLs to disk in parallel using aiohttp.
    url_name_pairs: [(url, save_as_name), ...]
    Returns: absolute paths of downloaded files.
    """
    connector = aiohttp.TCPConnector(limit=concurrency)
    tm = aiohttp.ClientTimeout(total=None if timeout is None else timeout)

    async with aiohttp.ClientSession(connector=connector, timeout=tm) as session:
        sem = asyncio.Semaphore(concurrency)
        results: List[str] = []

        async def one(u: str, name: str):
            path = os.path.join(out_dir, name)
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            async with sem:
                try:
                    async with session.get(u) as r:
                        r.raise_for_status()
                        async with aiofiles.open(path, "wb") as f:
                            async for chunk in r.content.iter_chunked(chunk_size):
                                await f.write(chunk)
                    results.append(os.path.abspath(path))
                    print(f"[DL OK] {name}")
                except Exception as e:
                    print(f"[DL ERR] {name}: {e}")

        await asyncio.gather(*(one(u, n) for (u, n) in url_name_pairs))
        return results


async def pipe_download_to_upload(
    client: httpx.AsyncClient,
    src_url: str,
    dest_url: str,
    method: str = "POST",
    chunk_size: int = 131_072,
    require_length: bool = False,
    extra_headers: Optional[Dict[str, str]] = None,
) -> Dict:
    """
    Stream download from src_url -> stream upload to dest_url without writing to disk.
    """
    async with client.stream("GET", src_url, follow_redirects=True) as r_get:
        r_get.raise_for_status()
        src_len = r_get.headers.get("Content-Length")
        headers = {"User-Agent": "saini-pipe/1.0"}
        if extra_headers:
            headers.update(extra_headers)

        # PUT endpoints like S3 may require Content-Length
        if method.upper() == "PUT":
            if src_len:
                headers["Content-Length"] = src_len
            elif require_length:
                raise RuntimeError("Upload requires Content-Length but source has none")

        async def body():
            async for chunk in r_get.aiter_bytes(chunk_size):
                yield chunk

        resp = await client.request(method, dest_url, content=body(), headers=headers)
        resp.raise_for_status()
        return {
            "src": src_url,
            "dest": dest_url,
            "status": resp.status_code,
            "bytes": int(src_len) if src_len else None,
        }


async def parallel_pipe(
    urls: List[str],
    upload_template: str,
    concurrency: int = 4,
    method: str = "POST",
    chunk_size: int = 131_072,
    require_length: bool = False,
) -> List[Dict]:
    """
    For each URL in urls, stream to upload_template.format(url, name).
    Example upload_template: "https://uploader.example.com/upload?name={name}"
    """
    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    async with httpx.AsyncClient(timeout=None, limits=limits) as client:
        sem = asyncio.Semaphore(concurrency)
        results: List[Dict] = []

        async def one(u: str):
            name = filename_from_url(u, "download.bin")
            dest = upload_template.format(url=u, name=name)
            async with sem:
                try:
                    res = await pipe_download_to_upload(
                        client, u, dest,
                        method=method,
                        chunk_size=chunk_size,
                        require_length=require_length
                    )
                    print(f"[PIPE OK] {name} -> {dest} ({res['status']})")
                    results.append(res)
                except Exception as e:
                    print(f"[PIPE ERR] {name}: {e}")
                    results.append({"src": u, "dest": dest, "error": str(e)})

        await asyncio.gather(*(one(u) for u in urls))
        return results


async def tg_upload_from_url(
    bot: Client,
    chat_id: Union[int, str],
    url: str,
    file_name: Optional[str] = None,
    caption: str = "",
    thumb: Optional[str] = None,
    chunk_size: int = 131_072,
):
    """
    Pipe-like Telegram upload: streams URL to a temp file (seekable) and uploads.
    Temp file is auto-deleted.
    """
    file_name = file_name or filename_from_url(url, "file.bin")
    note = await bot.send_message(chat_id, f"Downloading: {file_name}")
    start_time = time.time()

    # Create a named temp file path (so Pyrogram can open seekably)
    fd, temp_path = tempfile.mkstemp(prefix="pipe_", suffix=f"_{file_name}")
    os.close(fd)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                r.raise_for_status()
                async with aiofiles.open(temp_path, "wb") as f:
                    async for chunk in r.content.iter_chunked(chunk_size):
                        await f.write(chunk)

        # Upload with progress
        try:
            await bot.send_document(
                chat_id,
                document=temp_path,
                caption=caption,
                thumb=thumb,
                progress=progress_bar,
                progress_args=(note, start_time)
            )
        except Exception:
            # Fallback
            await bot.send_document(chat_id, document=temp_path, caption=caption)

    finally:
        try:
            await note.delete(True)
        except Exception:
            pass
        try:
            os.remove(temp_path)
        except Exception:
            pass


async def parallel_tg_upload_from_urls(
    bot: Client,
    chat_id: Union[int, str],
    items: List[Tuple[str, str]],  # [(url, file_name)]
    concurrency: int = 2,          # keep low to avoid flood-wait
    caption: str = "",
    thumb: Optional[str] = None
):
    sem = asyncio.Semaphore(concurrency)

    async def one(u: str, n: str):
        async with sem:
            await tg_upload_from_url(bot, chat_id, u, n, caption=caption, thumb=thumb)

    await asyncio.gather(*(one(u, n) for (u, n) in items))


# ======================= UPDATED: send_doc with URL support =======================

async def send_doc(bot: Client, m: Message, cc, ka, cc1, prog, count, name, channel_id):
    """
    If `ka` is a URL, it streams to a temp file and uploads (auto-deletes).
    If `ka` is a local path, it uploads and deletes the local file (as before).
    """
    reply = await bot.send_message(channel_id, f"Downloading pdf:\n<pre><code>{name}</code></pre>")
    await asyncio.sleep(1)
    start_time = time.time()
    try:
        if is_url(ka):
            # URL -> temp spool -> Telegram (pipe-like)
            await tg_upload_from_url(bot, channel_id, ka, file_name=name, caption=cc1)
        else:
            # Local file path
            try:
                await bot.send_document(
                    channel_id,
                    document=ka,
                    caption=cc1,
                    progress=progress_bar,
                    progress_args=(reply, start_time)
                )
            except Exception:
                await bot.send_document(channel_id, document=ka, caption=cc1)
            count += 1
            # cleanup local file if exists
            try:
                os.remove(ka)
            except Exception:
                pass
    finally:
        try:
            await reply.delete(True)
        except Exception:
            pass
    await asyncio.sleep(3)


def decrypt_file(file_path, key):
    if not os.path.exists(file_path):
        return False

    with open(file_path, "r+b") as f:
        num_bytes = min(28, os.path.getsize(file_path))
        with mmap.mmap(f.fileno(), length=num_bytes, access=mmap.ACCESS_WRITE) as mmapped_file:
            for i in range(num_bytes):
                mmapped_file[i] ^= ord(key[i]) if i < len(key) else i
    return True


async def download_and_decrypt_video(url, cmd, name, key):
    video_path = await download_video(url, cmd, name)

    if video_path:
        decrypted = decrypt_file(video_path, key)
        if decrypted:
            print(f"File {video_path} decrypted successfully.")
            return video_path
        else:
            print(f"Failed to decrypt {video_path}.")
            return None


async def send_vid(bot: Client, m: Message, cc, filename, vidwatermark, thumb, name, prog, channel_id):
    subprocess.run(f'ffmpeg -i "{filename}" -ss 00:00:10 -vframes 1 "{filename}.jpg"', shell=True)
    await prog.delete(True)
    reply1 = await bot.send_message(channel_id, f"**📩 Uploading Video 📩:-**\n<blockquote>**{name}**</blockquote>")
    reply = await m.reply_text(f"**Generate Thumbnail:**\n<blockquote>**{name}**</blockquote>")
    try:
        if thumb == "/d":
            thumbnail = f"{filename}.jpg"
        else:
            thumbnail = thumb

        if vidwatermark == "/d":
            w_filename = f"{filename}"
        else:
            w_filename = f"w_{filename}"
            font_path = "vidwater.ttf"
            subprocess.run(
                f'ffmpeg -i "{filename}" -vf "drawtext=fontfile={font_path}:text=\'{vidwatermark}\':fontcolor=white@0.3:fontsize=h/6:x=(w-text_w)/2:y=(h-text_h)/2" -codec:a copy "{w_filename}"',
                shell=True
            )

    except Exception as e:
        await m.reply_text(str(e))

    dur = int(duration(w_filename))
    start_time = time.time()

    try:
        await bot.send_video(
            channel_id,
            w_filename,
            caption=cc,
            supports_streaming=True,
            height=720,
            width=1280,
            thumb=thumbnail,
            duration=dur,
            progress=progress_bar,
            progress_args=(reply, start_time)
        )
    except Exception:
        await bot.send_document(
            channel_id,
            w_filename,
            caption=cc,
            progress=progress_bar,
            progress_args=(reply, start_time)
        )
    os.remove(w_filename)
    await reply.delete(True)
    await reply1.delete(True)
    os.remove(f"{filename}.jpg")


# ============================== OPTIONAL CLI ==============================
# Run: python saini.py pipe --urls-file src.txt --upload-template "https://uploader.example.com/upload?name={name}"
# Or: python saini.py dl --urls-file urls.txt --out downloads

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Parallel download + HTTP pipe helpers for saini.py")
    sub = ap.add_subparsers(dest="cmd")

    # parallel download to disk
    ap_dl = sub.add_parser("dl", help="Parallel download URLs to disk")
    ap_dl.add_argument("--urls", nargs="+", help="List of URLs")
    ap_dl.add_argument("--urls-file", help="File with one URL per line")
    ap_dl.add_argument("--out", default=".", help="Output directory")
    ap_dl.add_argument("--concurrency", type=int, default=8)
    ap_dl.add_argument("--chunk-size", type=int, default=131_072)

    # pipe streaming upload
    ap_pipe = sub.add_parser("pipe", help="Stream download->upload without disk")
    ap_pipe.add_argument("--urls", nargs="+", help="List of source URLs")
    ap_pipe.add_argument("--urls-file", help="File with one URL per line")
    ap_pipe.add_argument("--upload-template", required=True, help="Upload URL template; use {name} or {url}")
    ap_pipe.add_argument("--method", default="POST", help="Upload method: POST or PUT")
    ap_pipe.add_argument("--concurrency", type=int, default=4)
    ap_pipe.add_argument("--chunk-size", type=int, default=131_072)
    ap_pipe.add_argument("--require-length", action="store_true", help="Fail if Content-Length missing (e.g. S3 PUT)")

    args = ap.parse_args()

    if args.cmd == "dl":
        urls: List[str] = args.urls or []
        if args.urls_file:
            with open(args.urls_file) as f:
                urls += [line.strip() for line in f if line.strip()]
        if not urls:
            print("No URLs provided")
            raise SystemExit(1)
        # generate names from URL
        pairs = [(u, filename_from_url(u)) for u in urls]
        asyncio.run(parallel_download(pairs, out_dir=args.out, concurrency=args.concurrency, chunk_size=args.chunk_size))
    elif args.cmd == "pipe":
        urls: List[str] = args.urls or []
        if args.urls_file:
            with open(args.urls_file) as f:
                urls += [line.strip() for line in f if line.strip()]
        if not urls:
            print("No URLs provided")
            raise SystemExit(1)
        asyncio.run(
            parallel_pipe(
                urls=urls,
                upload_template=args.upload_template,
                concurrency=args.concurrency,
                method=args.method,
                chunk_size=args.chunk_size,
                require_length=args.require_length,
            )
        )
    else:
        ap.print_help()
