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

# --------------------- UTILITY FUNCTIONS ---------------------

def duration(filename):
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                             "format=duration", "-of",
                             "default=noprint_wrappers=1:nokey=1", filename],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT)
    return float(result.stdout)

def exec(cmd):
    process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.stdout.decode()
    print(output)
    return output

def pull_run(work, cmds):
    with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
        print("Waiting for tasks to complete")
        executor.map(exec, cmds)

def parse_vid_info(info):
    info = info.strip().split("\n")
    new_info, temp = [], []
    for i in info:
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i = i.strip().split("|")[0].split(" ", 2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except:
                pass
    return new_info

def vid_info(info):
    info = info.strip().split("\n")
    new_info, temp = dict(), []
    for i in info:
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i = i.strip().split("|")[0].split(" ", 3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.update({f'{i[2]}': f'{i[0]}'})
            except:
                pass
    return new_info

async def run(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    print(f'[{cmd!r} exited with {proc.returncode}]')
    if proc.returncode == 1:
        return False
    if stdout:
        return f'[stdout]\n{stdout.decode()}'
    if stderr:
        return f'[stderr]\n{stderr.decode()}'

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

def decrypt_file(file_path, key):  
    if not os.path.exists(file_path): 
        return False  
    with open(file_path, "r+b") as f:  
        num_bytes = min(28, os.path.getsize(file_path))  
        with mmap.mmap(f.fileno(), length=num_bytes, access=mmap.ACCESS_WRITE) as mmapped_file:  
            for i in range(num_bytes):  
                mmapped_file[i] ^= ord(key[i]) if i < len(key) else i 
    return True  

# --------------------- PARALLEL DOWNLOAD + PIPE UPLOAD ---------------------

async def download_file_to_memory(url):
    """Download file in memory using aiohttp"""
    buffer = BytesIO()
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                async for chunk in resp.content.iter_chunked(32 * 1024):
                    buffer.write(chunk)
                buffer.seek(0)
                return buffer
            else:
                return None

async def parallel_download_and_send(bot: Client, m: Message, urls, names, channel_id):
    """
    Downloads multiple files in parallel and uploads via pipe (BytesIO).
    """
    async def fetch(url, name):
        buf = await download_file_to_memory(url)
        return name, buf

    tasks = [fetch(url, name) for url, name in zip(urls, names)]
    results = await asyncio.gather(*tasks)

    for name, file_buffer in results:
        if file_buffer:
            await bot.send_document(
                chat_id=channel_id,
                document=file_buffer,
                file_name=name,
                caption=f"Uploaded: {name}"
            )
            file_buffer.close()
        else:
            await m.reply_text(f"Failed to download: {name}")

# --------------------- VIDEO DOWNLOAD + DECRYPT ---------------------

async def download_video(url, cmd, name, failed_counter=0):
    download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader aria2c --downloader-args "aria2c: -x 16 -j 32"'
    print(download_cmd)
    logging.info(download_cmd)
    k = subprocess.run(download_cmd, shell=True)
    if "visionias" in cmd and k.returncode != 0 and failed_counter <= 10:
        failed_counter += 1
        await asyncio.sleep(5)
        return await download_video(url, cmd, name, failed_counter)
    try:
        if os.path.isfile(name):
            return name
        for ext in ["webm", "mkv", "mp4", "mp4.webm"]:
            if os.path.isfile(f"{name}.{ext}"):
                return f"{name}.{ext}"
        return name
    except FileNotFoundError:
        return name

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
        await bot.send_video(channel_id, w_filename, caption=cc, supports_streaming=True,
                             height=720, width=1280, thumb=thumbnail, duration=dur,
                             progress=progress_bar, progress_args=(reply, start_time))
    except Exception:
        await bot.send_document(channel_id, w_filename, caption=cc,
                                progress=progress_bar, progress_args=(reply, start_time))
    os.remove(w_filename)
    await reply.delete(True)
    await reply1.delete(True)
    os.remove(f"{filename}.jpg")
