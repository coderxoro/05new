#🇳‌🇮‌🇰‌🇭‌🇮‌🇱‌
# Add your details here and then deploy by clicking on HEROKU Deploy button
import os
from os import environ

API_ID = int(environ.get("API_ID", "20344889"))
API_HASH = environ.get("API_HASH", "4e6e068135b733caf2496850989d401e")
BOT_TOKEN = environ.get("BOT_TOKEN", "8206073097:AAFcW-mpa_Duq3_kNC-cdsYPXyxL5WpnURc")

OWNER = int(environ.get("OWNER", "7636117585"))
CREDIT = environ.get("CREDIT", "𝘼𝙉𝙅𝘼𝙉 𝙋𝙀𝙍𝙎𝙊𝙉™")
cookies_file_path = os.getenv("cookies_file_path", "youtube_cookies.txt")

TOTAL_USER = os.environ.get('TOTAL_USERS', '7962530240').split(',')
TOTAL_USERS = [int(user_id) for user_id in TOTAL_USER]

AUTH_USER = os.environ.get('AUTH_USERS', '7962530240').split(',')
AUTH_USERS = [int(user_id) for user_id in AUTH_USER]
if int(OWNER) not in AUTH_USERS:
    AUTH_USERS.append(int(OWNER))

