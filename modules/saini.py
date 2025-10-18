import os
import re
import gc
import time
import mmap
import torch
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
import tgcrypto
import subprocess
import concurrent.futures
from math import ceil
from utils import progress_bar
from pyrogram import Client, filters
from pyrogram.types import Message
from io import BytesIO
from pathlib import Path  
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode
from functools import lru_cache
import tempfile
import shutil

# ============ PIPE DOWNLOADING METHODS ============

class PipelineDownloader:
    """Efficient pipeline downloading with caching"""
    
    def __init__(self, cache_dir="./cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=100,
                ttl_dns_cache=300,
                enable_cleanup_closed=True
            ),
            timeout=aiohttp.ClientTimeout(total=3600)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def download_with_pipe(self, url: str, filename: str, chunk_size: int = 8192):
        """Stream download with pipe method for memory efficiency"""
        try:
            output_path = self.cache_dir / filename
            
            # Check if already cached
            if output_path.exists():
                logging.info(f"Using cached file: {filename}")
                return str(output_path)
            
            # Create temp file for atomic write
            temp_file = tempfile.NamedTemporaryFile(delete=False, dir=self.cache_dir)
            
            async with self.session.get(url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                
                logging.info(f"Downloading {filename}: {human_readable_size(total_size)}")
                
                downloaded = 0
                async with aiofiles.open(temp_file.name, 'wb') as file:
                    async for chunk in response.content.iter_chunked(chunk_size):
                        await file.write(chunk)
                        downloaded += len(chunk)
                        
                        # Free memory periodically
                        if downloaded % (chunk_size * 100) == 0:
                            gc.collect()
            
            # Atomic move to final location
            shutil.move(temp_file.name, output_path)
            logging.info(f"Downloaded successfully: {filename}")
            return str(output_path)
            
        except Exception as e:
            logging.error(f"Download failed: {e}")
            if temp_file and os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise
    
    async def parallel_download(self, urls: list, max_workers: int = 5):
        """Download multiple files in parallel"""
        tasks = []
        for i, url in enumerate(urls):
            filename = f"file_{i}_{int(time.time())}.tmp"
            task = self.download_with_pipe(url, filename)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]

# ============ OPTIMIZED VIDEO PROCESSING ============

class VideoProcessor:
    """Optimized video processing with streaming"""
    
    @staticmethod
    async def stream_process(input_file: str, output_file: str, watermark: str = None):
        """Process video in streaming mode to save memory"""
        cmd = ['ffmpeg', '-i', input_file]
        
        if watermark:
            font_path = "vidwater.ttf"
            cmd.extend([
                '-vf',
                f"drawtext=fontfile={font_path}:text='{watermark}':fontcolor=white@0.3:fontsize=h/6:x=(w-text_w)/2:y=(h-text_h)/2"
            ])
        
        cmd.extend([
            '-c:v', 'libx264',  # Use H.264 codec
            '-preset', 'fast',   # Faster encoding
            '-crf', '23',        # Quality setting
            '-c:a', 'copy',      # Copy audio without re-encoding
            '-movflags', '+faststart',  # Enable streaming
            output_file
        ])
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"FFmpeg error: {stderr.decode()}")
        
        return output_file

# ============ MEMORY OPTIMIZED FUNCTIONS ============

@lru_cache(maxsize=128)
def duration(filename):
    """Cached duration check"""
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries",
         "format=duration", "-of",
         "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30
    )
    return float(result.stdout)

def exec_with_timeout(cmd, timeout=300):
    """Execute command with timeout"""
    try:
        process = subprocess.run(
            cmd, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            shell=True if isinstance(cmd, str) else False
        )
        output = process.stdout.decode()
        print(output)
        return output
    except subprocess.TimeoutExpired:
        logging.error(f"Command timed out: {cmd}")
        return None

async def pull_run_async(work, cmds):
    """Async version of parallel execution"""
    tasks = []
    for cmd in cmds:
        task = asyncio.create_task(run(cmd))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results

# ============ OPTIMIZED DOWNLOAD FUNCTIONS ============

async def aio(url, name):
    """Optimized async download with pipe"""
    async with PipelineDownloader() as downloader:
        return await downloader.download_with_pipe(url, f'{name}.pdf')

async def download(url, name):
    """Memory efficient download"""
    ka = f'{name}.pdf'
    
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(force_close=True)
    ) as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                # Stream to file instead of loading in memory
                async with aiofiles.open(ka, mode='wb') as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        await f.write(chunk)
                        
                # Clear memory after chunks
                gc.collect()
    return ka

# ============ KEEP ORIGINAL FUNCTIONS (OPTIMIZED) ============

def parse_vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = []
    temp = set()  # Use set for faster lookup
    
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            i = re.sub(r'\s+', ' ', i)  # Regex for multiple spaces
            i = i.strip()
            parts = i.split("|")[0].split(" ", 2)
            
            try:
                if len(parts) > 2:
                    resolution = parts[2]
                    if ("RESOLUTION" not in resolution and 
                        resolution not in temp and 
                        "audio" not in resolution):
                        temp.add(resolution)
                        new_info.append((parts[0], resolution))
            except:
                pass
    
    return new_info

def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = set()
    
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            i = re.sub(r'\s+', ' ', i)
            i = i.strip()
            parts = i.split("|")[0].split(" ", 3)
            
            try:
                if len(parts) > 2:
                    resolution = parts[2]
                    if ("RESOLUTION" not in resolution and 
                        resolution not in temp and 
                        "audio" not in resolution):
                        temp.add(resolution)
                        new_info[resolution] = parts[0]
            except:
                pass
    
    return new_info

async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    """Optimized decrypt and merge with cleanup"""
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Use pipe downloader for MPD
        async with PipelineDownloader(cache_dir=output_path) as downloader:
            # Download with yt-dlp
            cmd1 = f'yt-dlp -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --external-downloader aria2c --downloader-args "aria2c:-x 16 -s 16 -k 1M" "{mpd_url}"'
            print(f"Running command: {cmd1}")
            
            proc = await asyncio.create_subprocess_shell(
                cmd1,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
        
        avDir = list(output_path.iterdir())
        print(f"Downloaded files: {avDir}")
        
        # Parallel decryption
        decrypt_tasks = []
        
        for data in avDir:
            if data.suffix == ".mp4":
                cmd = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                decrypt_tasks.append(run(cmd))
            elif data.suffix == ".m4a":
                cmd = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                decrypt_tasks.append(run(cmd))
        
        if decrypt_tasks:
            await asyncio.gather(*decrypt_tasks)
        
        # Clean up encrypted files
        for data in avDir:
            if data.suffix in [".mp4", ".m4a"]:
                data.unlink()
        
        # Merge with streaming
        processor = VideoProcessor()
        filename = output_path / f"{output_name}.mp4"
        
        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy -movflags +faststart "{filename}"'
        await run(cmd4)
        
        # Cleanup
        for file in ["video.mp4", "audio.m4a"]:
            file_path = output_path / file
            if file_path.exists():
                file_path.unlink()
        
        gc.collect()  # Force garbage collection
        
        return str(filename)
        
    except Exception as e:
        print(f"Error during decryption and merging: {str(e)}")
        raise
    finally:
        # Ensure cleanup happens
        gc.collect()

async def run(cmd):
    """Optimized async command execution"""
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
    """Streaming download with progress"""
    if os.path.exists(file_name):
        os.remove(file_name)
    
    with requests.get(url, allow_redirects=True, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        
        with open(file_name, 'wb') as fd:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fd.write(chunk)
                    downloaded += len(chunk)
                    
                    # Free memory periodically
                    if downloaded % (chunk_size * 100) == 0:
                        gc.collect()
    
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
    """Optimized video download with retries"""
    download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader aria2c --downloader-args "aria2c: -x 16 -s 16 -k 1M --file-allocation=none"'
    
    global failed_counter
    failed_counter = getattr(download_video, 'failed_counter', 0)
    
    print(download_cmd)
    logging.info(download_cmd)
    
    # Use async subprocess
    proc = await asyncio.create_subprocess_shell(
        download_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    
    if "visionias" in cmd and proc.returncode != 0 and failed_counter <= 10:
        failed_counter += 1
        download_video.failed_counter = failed_counter
        await asyncio.sleep(5)
        return await download_video(url, cmd, name)
    
    download_video.failed_counter = 0
    
    # Check for various output formats
    possible_names = [
        name,
        f"{name}.webm",
        f"{name.split('.')[0]}.mkv",
        f"{name.split('.')[0]}.mp4",
        f"{name.split('.')[0]}.mp4.webm"
    ]
    
    for possible_name in possible_names:
        if os.path.isfile(possible_name):
            return possible_name
    
    return name

async def send_doc(bot: Client, m: Message, cc, ka, cc1, prog, count, name, channel_id):
    """Optimized document sending"""
    reply = await bot.send_message(
        channel_id, 
        f"Downloading pdf:\n<pre><code>{name}</code></pre>"
    )
    
    await asyncio.sleep(1)
    
    try:
        await bot.send_document(ka, caption=cc1)
        count += 1
    finally:
        await reply.delete(True)
        await asyncio.sleep(1)
        
        if os.path.exists(ka):
            os.remove(ka)
        
        gc.collect()  # Clean memory
    
    await asyncio.sleep(3)
    return count

def decrypt_file(file_path, key):
    """Memory-mapped file decryption"""
    if not os.path.exists(file_path):
        return False
    
    try:
        with open(file_path, "r+b") as f:
            file_size = os.path.getsize(file_path)
            num_bytes = min(28, file_size)
            
            # Use context manager for mmap
            with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_WRITE) as mmapped_file:
                for i in range(num_bytes):
                    current_byte = mmapped_file[i]
                    key_byte = ord(key[i]) if i < len(key) else i
                    mmapped_file[i] = current_byte ^ key_byte
        
        return True
    except Exception as e:
        logging.error(f"Decryption error: {e}")
        return False

async def download_and_decrypt_video(url, cmd, name, key):
    """Async download and decrypt"""
    video_path = await download_video(url, cmd, name)
    
    if video_path:
        # Run decryption in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        decrypted = await loop.run_in_executor(None, decrypt_file, video_path, key)
        
        if decrypted:
            print(f"File {video_path} decrypted successfully.")
            return video_path
        else:
            print(f"Failed to decrypt {video_path}.")
            return None

async def send_vid(bot: Client, m: Message, cc, filename, vidwatermark, thumb, name, prog, channel_id):
    """Optimized video sending with streaming"""
    try:
        # Generate thumbnail asynchronously
        thumb_cmd = f'ffmpeg -i "{filename}" -ss 00:00:10 -vframes 1 -q:v 2 "{filename}.jpg"'
        await run(thumb_cmd)
        
        await prog.delete(True)
        
        reply1 = await bot.send_message(
            channel_id, 
            f"**📩 Uploading Video 📩:-**\n<blockquote>**{name}**</blockquote>"
        )
        reply = await m.reply_text(
            f"**Generate Thumbnail:**\n<blockquote>**{name}**</blockquote>"
        )
        
        # Set thumbnail
        thumbnail = f"{filename}.jpg" if thumb == "/d" else thumb
        
        # Process watermark if needed
        if vidwatermark == "/d":
            w_filename = filename
        else:
            processor = VideoProcessor()
            w_filename = f"w_{filename}"
            await processor.stream_process(filename, w_filename, vidwatermark)
        
        # Get duration
        dur = int(duration(w_filename))
        start_time = time.time()
        
        # Upload with streaming support
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
        
    except Exception as e:
        await m.reply_text(f"Error: {str(e)}")
    finally:
        # Cleanup
        files_to_remove = [w_filename, f"{filename}.jpg"]
        for file in files_to_remove:
            if os.path.exists(file):
                os.remove(file)
        
        if reply:
            await reply.delete(True)
        if reply1:
            await reply1.delete(True)
        
        gc.collect()  # Force garbage collection

# ============ CLEANUP FUNCTION ============

async def cleanup_old_files(directory="./cache", days=7):
    """Clean old cached files"""
    now = time.time()
    cutoff = now - (days * 86400)
    
    for file in Path(directory).glob("*"):
        if file.is_file() and file.stat().st_mtime < cutoff:
            file.unlink()
            logging.info(f"Deleted old file: {file}")

# Initialize cleanup task
async def init_cleanup():
    while True:
        await cleanup_old_files()
        await asyncio.sleep(86400)  # Run daily
