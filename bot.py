#pylint:disable=E0401
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import aiofiles
import os
import re
import time
import asyncio
import tempfile
import sqlite3
import zipfile
import libtorrent as lt
import logging
import json
import shutil # Para copiar archivos
import tgcrypto
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin

import aiohttp
from bs4 import BeautifulSoup
from pyrogram import Client, idle, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery




# --- Google Drive Imports ---
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# ——— CONFIG —————————————————————————————————————————————————————
BOT_TOKEN     = "6795390784:AAHkO93ULck3V7ByutnWUhT319dMiHamuFs" # ¡IMPORTANT! Replace with your bot token
API_ID        = 9548711 # ¡IMPORTANT! Replace with your API ID
API_HASH      = "4225fbfa50c5ac44194081a0f114bdd1" # ¡IMPORTANT! Replace with your API Hash
ADMIN_IDS     = [6829735291, 1126236383] # ¡IMPORTANT! Replace with your Telegram user ID
DB_PATH       = "bot_data.db"

MAX_TG_FILE   = 2 * 1024**3       # 2 GB
SPLIT_PART    = int(1.95 * 1024**3)  # 1.95 GB

# User-Agent for HTTP requests (web scraping)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='bot.log', filemode='a')

bot = Client("bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# --- Google Drive Config ---
GOOGLE_DRIVE_SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.metadata.readonly'
]
CLIENT_SECRET_FILE = 'client_secret.json'
TOKEN_PICKLE_DIR = 'tokens'
os.makedirs(TOKEN_PICKLE_DIR, exist_ok=True)

# --- Language Config ---
LANG_DIR = "lang"
DEFAULT_LANG = "es"
AVAILABLE_LANGS = {
    "es": "Español 🇪🇸",
    "en": "English 🇬🇧",
    "fr" : "Frances🇫🇷"
}
LANG_DATA = {}

active_downloads_update_tasks: dict[str, asyncio.Task] = {}
user_detail_update_tasks: dict[int, asyncio.Task] = {}


def load_languages():
    """Loads language files from the LANG_DIR folder."""
    os.makedirs(LANG_DIR, exist_ok=True)
    for lang_code in AVAILABLE_LANGS.keys():
        file_path = os.path.join(LANG_DIR, f"{lang_code}.json")
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                LANG_DATA[lang_code] = json.load(f)
        else:
            logging.warning(f"Language file not found: {file_path}. Please create it.")

load_languages()

# ——— GLOBAL STATE ———————————————————————————————————————————————
active_downloads: dict[str, dict] = {}
MAINTENANCE_MODE = False
SEARCH_DISABLED  = False
user_search_states: dict[int, dict] = {}
broadcast_state: dict[int, str] = {}

# Download queue
download_queue = asyncio.Queue()
# Dictionary to store ongoing download tasks
processing_downloads: dict[str, asyncio.Task] = {}
# Limit for simultaneous downloads
MAX_SIMULTANEOUS_DOWNLOADS = 2

# ——— UTILS AND HELPERS —————————————————————————————————————————————
def separator() -> str:
    return "──────────"

def format_bytes(size: float) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024
    return f"{size:.2f}{unit}"

def get_user_stats(user_id: int) -> tuple[str,int]:
    """Returns (username, total_downloads)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    username = row[0] if row else f"user{user_id}"
    cur.execute("SELECT COUNT(*) FROM user_torrents WHERE user_id=?", (user_id,))
    count = cur.fetchone()[0]
    conn.close()
    return username, count

def get_profile_header(user_id: int) -> str:
    user, cnt = get_user_stats(user_id)
    return f"👤 @{user} • 📚 {cnt} {get_text(user_id, 'downloads_count_text')}\n{separator()}"

def build_main_menu(user_id: int) -> InlineKeyboardMarkup:
    header = get_profile_header(user_id)
    buttons = [
        [InlineKeyboardButton(get_text(user_id, "main_menu_search"), callback_data="menu:search")],
        [InlineKeyboardButton(get_text(user_id, "main_menu_downloads"), callback_data="menu:downloads")],
        [InlineKeyboardButton(get_text(user_id, "main_menu_settings"), callback_data="menu:settings")],
        [InlineKeyboardButton(get_text(user_id, "main_menu_help"), callback_data="menu:help")],
        [InlineKeyboardButton(get_text(user_id, "main_menu_exit"), callback_data="menu:exit")],
    ]
    markup = InlineKeyboardMarkup(buttons)
    return header, markup

def build_admin_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(get_text(user_id, "admin_downloads"), callback_data="admin:dl_page:0")],
        [InlineKeyboardButton(get_text(user_id, "admin_stats"), callback_data="admin:stats")],
        [InlineKeyboardButton(get_text(user_id, "admin_queue"), callback_data="admin:queue")],
        [InlineKeyboardButton(get_text(user_id, "admin_maintenance"), callback_data="admin:maintenance")],
        [InlineKeyboardButton(get_text(user_id, "admin_management"), callback_data="admin:management")], # New button for Management
    ]
    return InlineKeyboardMarkup(buttons)

def build_settings_menu(user_id: int) -> InlineKeyboardMarkup:
    theme_icon = "🌓"
    current_upload_mode = get_user_upload_mode(user_id)
    
    buttons = [
        [InlineKeyboardButton(f"{theme_icon} {get_text(user_id, 'settings_toggle_theme')}", callback_data="settings:toggle_theme")],
        [InlineKeyboardButton(get_text(user_id, "settings_upload_mode", mode=current_upload_mode.capitalize()), callback_data="settings:upload_mode")],
        [InlineKeyboardButton(get_text(user_id, "settings_notifications"), callback_data="settings:notifications")],
        [InlineKeyboardButton(get_text(user_id, "settings_clear_history"), callback_data="settings:clear_history")],
        [InlineKeyboardButton(get_text(user_id, "language_settings"), callback_data="settings:language")],
        [InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data="menu:back_main")],
    ]
    return InlineKeyboardMarkup(buttons)

def build_management_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(get_text(user_id, "admin_blocked_users"), callback_data="admin:users")],
        [InlineKeyboardButton(get_text(user_id, "admin_all_users"), callback_data="admin:all_users_page:0")], # New button for all users
        [InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data="admin:back_main")],
    ]
    return InlineKeyboardMarkup(buttons)

# NEW: Function to build download detail text
def _build_dl_detail_text(user_id: int, dl_info: dict) -> str:
    s = dl_info["handle"].status()
    prog = s.progress * 100
    
    eta_val = get_text(user_id, "not_available_abbr")
    if s.download_rate > 0 and dl_info["total_size_bytes"] > 0:
        remaining_bytes = dl_info["total_size_bytes"] - s.total_done
        if remaining_bytes > 0:
            eta_val = calculate_eta(dl_info["start"], dl_info["total_size_bytes"], s.total_done)
    
    bar = make_progress_bar(s.total_done, dl_info["total_size_bytes"])

    return (
        f"**{get_text(user_id, 'admin_download_detail_title', title=dl_info['title'])}**\n\n"
        f"👤 {get_text(user_id, 'admin_download_detail_user', username=dl_info['username'])}\n"
        f"📦 {get_text(user_id, 'admin_download_detail_total_size', size=format_bytes(dl_info['total_size_bytes']))}\n"
        f"📊 {get_text(user_id, 'admin_download_detail_progress', bar=bar, progress=f'{prog:.1f}')}%\n"
        f"⏳ {get_text(user_id, 'admin_download_detail_eta', eta=eta_val)}\n"
        f"⬇️ {get_text(user_id, 'admin_download_detail_downloaded', downloaded=format_bytes(s.total_done))}\n"
        f"⬆️ {get_text(user_id, 'admin_download_detail_uploaded', uploaded=format_bytes(s.total_upload))}\n"
        f"📈 {get_text(user_id, 'admin_download_detail_speed', speed=format_bytes(s.download_rate))}/s\n"
        f"👥 {get_text(user_id, 'admin_download_detail_peers', peers=s.num_peers)}\n"
        f"🆔 {dl_info['id']}"
    )

def _build_dl_detail_buttons(user_id: int, dl_id: str, dl_info: dict, page: int) -> InlineKeyboardMarkup:
    pause_resume_button = InlineKeyboardButton(
        get_text(user_id, "admin_resume_download") if dl_info["paused"] else get_text(user_id, "admin_pause_download"),
        callback_data=f"admin:action:toggle_pause_resume:{dl_id}:{page}"
    )
    buttons = [
        [pause_resume_button, InlineKeyboardButton(get_text(user_id, "admin_cancel_download"), callback_data=f"admin:action:confirm_cancel:{dl_id}:{page}")],
        [InlineKeyboardButton(get_text(user_id, "admin_block_user"), callback_data=f"admin:action:confirm_block:{dl_info['user_id']}:{page}")],
        [InlineKeyboardButton(get_text(user_id, "admin_files"), callback_data=f"admin:action:preview:{dl_id}:{page}")],
        [InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data=f"admin:dl_page:{page}")]
    ]
    return InlineKeyboardMarkup(buttons)

def _build_user_detail_text(user_id: int, user_data: dict) -> str:
    is_user_blocked = is_blocked(user_data['user_id'])
    status_text = get_text(user_id, "admin_user_status_blocked") if is_user_blocked else get_text(user_id, "admin_user_status_active")
    
    return (
        f"**👤 {get_text(user_id, 'admin_user_detail_title', username=user_data['username'] or f'user{user_data['user_id']}')}**\n\n"
        f"ID de Usuario: `{user_data['user_id']}`\n"
        f"Unido el: {datetime.fromisoformat(user_data['joined_at']).strftime('%d/%m/%Y %H:%M')}\n"
        f"Total Descargas: {user_data['total_downloads']}\n"
        f"Modo de Subida: {user_data['upload_mode'].capitalize()}\n"
        f"Idioma: {AVAILABLE_LANGS.get(user_data['language_code'], user_data['language_code'])}\n"
        f"Estado: {status_text}"
    )
    
def _build_user_detail_buttons(admin_id: int, target_user_id: int, current_page: int) -> InlineKeyboardMarkup:
    is_target_blocked = is_blocked(target_user_id)
    buttons = []
    if is_target_blocked:
        buttons.append([InlineKeyboardButton(get_text(admin_id, "admin_unblock_user"), callback_data=f"admin:action:confirm_unblock:{target_user_id}:{current_page}")])
    else:
        buttons.append([InlineKeyboardButton(get_text(admin_id, "admin_block_user"), callback_data=f"admin:action:confirm_block:{target_user_id}:{current_page}")])
    
    buttons.append([InlineKeyboardButton(get_text(admin_id, "settings_back"), callback_data=f"admin:all_users_page:{current_page}")])
    return InlineKeyboardMarkup(buttons)

async def update_dl_detail_periodically(message: Message, user_id: int, dl_id: str, page: int):
    try:
        while dl_id in active_downloads:
            dl_info = active_downloads.get(dl_id)
            if not dl_info or dl_info.get("cancelled") or dl_info.get("paused"):
                break
            
            new_text = _build_dl_detail_text(user_id, dl_info)
            new_markup = _build_dl_detail_buttons(user_id, dl_id, dl_info, page)

            # Only edit if the text or markup has changed
            if message.text != new_text or message.reply_markup != new_markup:
                try:
                    await message.edit_text(new_text, reply_markup=new_markup)
                except Exception as e:
                    logging.warning(f"Error editing download detail message {dl_id}: {e}")
                    break
            await asyncio.sleep(5) # Update every 5 seconds
    except asyncio.CancelledError:
        logging.info(f"Update task for {dl_id} cancelled.")
    except Exception as e:
        logging.error(f"Unexpected error in update task for {dl_id}: {e}", exc_info=True)
    finally:
        active_downloads_update_tasks.pop(dl_id, None) # Clean up the task when finished



async def update_user_detail_periodically(message: Message, admin_id: int, target_user_id: int, page: int):
    try:
        while True: # Updates until navigated away or task cancelled
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT user_id, username, joined_at, upload_mode, language_code FROM users WHERE user_id=?", (target_user_id,))
            user_data = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM user_torrents WHERE user_id=?", (target_user_id,))
            total_downloads = cur.fetchone()[0]
            conn.close()

            if not user_data:
                break # User not found, stop updating

            user_info = {
                'user_id': user_data[0],
                'username': user_data[1],
                'joined_at': user_data[2],
                'upload_mode': user_data[3],
                'language_code': user_data[4],
                'total_downloads': total_downloads
            }
            
            new_text = _build_user_detail_text(admin_id, user_info)
            new_markup = _build_user_detail_buttons(admin_id, target_user_id, page)

            if message.text != new_text or message.reply_markup != new_markup:
                try:
                    await message.edit_text(new_text, reply_markup=new_markup)
                except Exception as e:
                    logging.warning(f"Error editing user detail message {target_user_id}: {e}")
                    break
            await asyncio.sleep(10) # Update every 10 seconds
    except asyncio.CancelledError:
        logging.info(f"Update task for user {target_user_id} cancelled.")
    except Exception as e:
        logging.error(f"Unexpected error in user update task for {target_user_id}: {e}", exc_info=True)
    finally:
        user_detail_update_tasks.pop(target_user_id, None)


def build_upload_mode_menu(user_id: int) -> InlineKeyboardMarkup:
    current_mode = get_user_upload_mode(user_id)
    buttons = [
        [InlineKeyboardButton(f"{get_text(user_id, 'upload_mode_telegram')} {'✅' if current_mode == 'telegram' else ''}", callback_data="settings:set_upload_mode:telegram")],
        [InlineKeyboardButton(f"{get_text(user_id, 'upload_mode_drive')} {'✅' if current_mode == 'drive' else ''}", callback_data="settings:set_upload_mode:drive")],
        [InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data="menu:settings")]
    ]
    return InlineKeyboardMarkup(buttons)

def build_language_menu(user_id: int) -> InlineKeyboardMarkup:
    current_lang = get_user_language(user_id)
    buttons = []
    for lang_code, lang_name in AVAILABLE_LANGS.items():
        buttons.append([InlineKeyboardButton(f"{lang_name} {'✅' if current_lang == lang_code else ''}", callback_data=f"settings:set_language:{lang_code}")])
    buttons.append([InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data="menu:settings")])
    return InlineKeyboardMarkup(buttons)

def build_help_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(get_text(user_id, "help_commands"), callback_data="help:commands")],
        [InlineKeyboardButton(get_text(user_id, "help_faq"), callback_data="help:faq")],
        [InlineKeyboardButton(get_text(user_id, "help_support"), url="https://t.me/Nanatsu2370")],
        [InlineKeyboardButton(get_text(user_id, "settings_back"), callback_data="menu:back_main")],
    ]
    return InlineKeyboardMarkup(buttons)

# ——— DATABASE ——————————————————————————————————————————————


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executescript("""
    -- 🧑 Usuarios
    CREATE TABLE IF NOT EXISTS users (
      user_id   INTEGER PRIMARY KEY,
      username  TEXT,
      joined_at TEXT,
      is_new_user BOOLEAN DEFAULT TRUE,
      upload_mode TEXT DEFAULT 'telegram',
      language_code TEXT DEFAULT 'es'
    );

    -- 📥 Torrents de usuarios
    CREATE TABLE IF NOT EXISTS user_torrents (
      id        INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id   INTEGER NOT NULL,
      title     TEXT NOT NULL,
      magnet    TEXT NOT NULL,
      size      TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      added_at  TEXT NOT NULL
    );

    -- 🔐 Lista de administradores
    CREATE TABLE IF NOT EXISTS admins (
      user_id INTEGER PRIMARY KEY
    );

    -- 🚫 Usuarios bloqueados
    CREATE TABLE IF NOT EXISTS blocked_users (
      user_id INTEGER PRIMARY KEY
    );

    -- 🔑 Tokens de Google Drive
    CREATE TABLE IF NOT EXISTS google_drive_tokens (
      user_id INTEGER PRIMARY KEY,
      access_token TEXT NOT NULL,
      refresh_token TEXT NOT NULL,
      token_uri TEXT NOT NULL,
      client_id TEXT NOT NULL,
      client_secret TEXT NOT NULL,
      scopes TEXT NOT NULL,
      expiration_time TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    -- 📁 Carpetas de Drive
    CREATE TABLE IF NOT EXISTS google_drive_folders (
      user_id INTEGER PRIMARY KEY,
      folder_id TEXT NOT NULL,
      folder_name TEXT NOT NULL,
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    -- ⏳ Cola de descargas
    CREATE TABLE IF NOT EXISTS download_queue (
      queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      chat_id INTEGER NOT NULL,
      magnet_link TEXT NOT NULL,
      title TEXT NOT NULL,
      status_message_id INTEGER,
      added_at TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending' -- pending, processing, completed, failed, cancelled
    );

    -- 🗂️ Clasificaciones de productos
    CREATE TABLE IF NOT EXISTS shop_categories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE
    );

    -- 🛍️ Productos del marketplace
    CREATE TABLE IF NOT EXISTS shop_products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      desc TEXT,
      price TEXT NOT NULL,
      admin_id INTEGER NOT NULL,
      admin_username TEXT NOT NULL,
      custom_msg TEXT,
      category_id INTEGER NOT NULL,
      image_file_id TEXT,
      FOREIGN KEY (category_id) REFERENCES shop_categories(id)
    );
    """)
    conn.commit()
    conn.close()
    add_admins()

def add_admins():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for aid in ADMIN_IDS:
        cur.execute("INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (aid,))
    conn.commit()
    conn.close()

def is_admin(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,))
    res = cur.fetchone() is not None
    conn.close()
    return res

def is_blocked(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM blocked_users WHERE user_id=?", (user_id,))
    res = cur.fetchone() is not None
    conn.close()
    return res

def block_user_db(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO blocked_users(user_id) VALUES(?)", (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error blocking user {user_id} in DB: {e}")
        return False
    finally:
        conn.close()

def unblock_user_db(user_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM blocked_users WHERE user_id=?", (user_id,))
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Error unblocking user {user_id} in DB: {e}")
        return False
    finally:
        conn.close()

def get_user_upload_mode(user_id: int) -> str:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT upload_mode FROM users WHERE user_id=?", (user_id,))
    mode = cur.fetchone()
    conn.close()
    return mode[0] if mode else 'telegram'

def set_user_upload_mode(user_id: int, mode: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE users SET upload_mode=? WHERE user_id=?", (mode, user_id))
    conn.commit()
    conn.close()

def get_user_language(user_id: int) -> str:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT language_code FROM users WHERE user_id=?", (user_id,))
    lang = cur.fetchone()
    conn.close()
    return lang[0] if lang and lang[0] in AVAILABLE_LANGS else DEFAULT_LANG

def set_user_language(user_id: int, lang_code: str):
    if lang_code not in AVAILABLE_LANGS:
        logging.warning(f"Attempted to set invalid language code: {lang_code} for user {user_id}")
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE users SET language_code=? WHERE user_id=?", (lang_code, user_id))
    conn.commit()
    conn.close()

def get_text(user_id: int, key: str, **kwargs) -> str:
    """
    Gets the translated text for a given key and user.
    If the key is not found in the user's language, it tries the default language.
    """
    lang_code = get_user_language(user_id)
    text = LANG_DATA.get(lang_code, {}).get(key, LANG_DATA.get(DEFAULT_LANG, {}).get(key, f"MISSING_TEXT: {key}"))
    try:
        return text.format(**kwargs)
    except KeyError as e:
        logging.error(f"Missing format key {e} for text '{text}' in language '{lang_code}' for key '{key}'")
        return text

def format_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"

def tail_logs(path: str, lines: int = 10) -> str:
    if not os.path.exists(path):
        return get_text(0, "no_logs_found")
    with open(path, 'rb') as f:
        data = f.readlines()[-lines:]
    return "".join(l.decode(errors='ignore') for l in data)

def validate_magnet_link(magnet: str) -> bool:
    """Validates that the magnet link has the correct format"""
    pattern = r'^magnet:\?xt=urn:btih:[a-fA-F0-9]{40,}.*'
    return bool(re.match(pattern, magnet))

# Helpers for UI and ETA calculation
def calculate_eta(start_time, total, done):
    elapsed = time.time() - start_time
    if done == 0 or elapsed == 0:
        return get_text(0, "calculating_eta")
    rate = done / elapsed
    rem = total - done
    secs = int(rem / rate)
    m, s = divmod(secs, 60)
    return f"{m}m {s}s"

def render_progress_ui(user_id: int, fname: str, user: str, state: str, speed: str, eta: str):
    return (
        f"📁 {get_text(user_id, 'file_name_label')}: `{fname}`\n"
        f"🔐 {get_text(user_id, 'user_label')}: `{user}`\n"
        f"📊 {get_text(user_id, 'status_label')}: `{state}`\n"
        f"📈 {get_text(user_id, 'speed_label')}: `{speed}`\n"
        f"⏳ {get_text(user_id, 'eta_label')}: `{eta}`"
    )

def render_paused_ui(user_id: int, fname: str, user: str):
    return (
        f"📁 {get_text(user_id, 'file_name_label')}: `{fname}`\n"
        f"🔐 {get_text(user_id, 'user_label')}: `{user}`\n"
        f"📊 {get_text(user_id, 'status_label')}: `{get_text(user_id, 'download_paused_status')}`\n"
        f"📈 {get_text(user_id, 'speed_label')}: `0 KB/s`\n"
        f"⏳ {get_text(user_id, 'resume_prompt')}`"
    )

def make_progress_bar(current: int, total: int, length: int = 12) -> str:
    """
    Creates a progress bar of 'length' width, e.g. [██████----] 50.0%
    """
    proportion = current / total
    filled     = int(proportion * length)
    empty      = length - filled
    percent    = proportion * 100
    bar        = "█" * filled + "-" * empty
    return f"[{bar}] {percent:5.1f}%"

async def _progress_callback(
    current: int,
    total: int,
    status_msg: Message,
    prefix: str
):
    """
    Callback invoked during upload.
    Edits every 5 seconds and only if the text has changed.
    """
    bar = make_progress_bar(current, total)
    new_text = (
        f"{prefix}\n"
        f"{bar}\n"
        f"{format_bytes(current)}/{format_bytes(total)}"
    )

    now = time.time()
    if not hasattr(status_msg, "_last_edit_time"):
        status_msg._last_edit_time = 0.0
        status_msg._last_text      = ""

    if (now - status_msg._last_edit_time >= 5.0
            and new_text != status_msg._last_text):
        await status_msg.edit(new_text)
        status_msg._last_edit_time = now
        status_msg._last_text      = new_text





# --- Google Drive Utils ---
def get_google_drive_credentials(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT access_token, refresh_token, token_uri, client_id, client_secret, scopes, expiration_time FROM google_drive_tokens WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        access_token, refresh_token, token_uri, client_id, client_secret, scopes_str, expiration_time_str = row
        scopes = scopes_str.split(',')
        expiration_time = datetime.fromisoformat(expiration_time_str)
        
        creds = Credentials(
            token=access_token,
            refresh_token=refresh_token,
            token_uri=token_uri,
            client_id=client_id,
            client_secret=client_secret,
            scopes=scopes
        )
        creds.expiry = expiration_time
        return creds
    return None

def save_google_drive_credentials(user_id: int, creds: Credentials):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    scopes_str = ','.join(creds.scopes)
    expiration_time_str = creds.expiry.isoformat() if creds.expiry else None

    cur.execute("""
        INSERT OR REPLACE INTO google_drive_tokens
        (user_id, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expiration_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, creds.token, creds.refresh_token, creds.token_uri, creds.client_id, creds.client_secret, scopes_str, expiration_time_str))
    conn.commit()
    conn.close()

async def get_authenticated_drive_service(user_id: int, message: Message):
    creds = get_google_drive_credentials(user_id)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                save_google_drive_credentials(user_id, creds)
                logging.info(f"Drive credentials refreshed for user {user_id}")
            except Exception as e:
                logging.error(f"Error refreshing Drive credentials for {user_id}: {e}")
                await message.reply(get_text(user_id, "drive_auth_expired"))
                return None
        else:
            return None

    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        logging.error(f"Error building Drive service for {user_id}: {e}")
        await message.reply(get_text(user_id, "drive_connection_error"))
        return None

async def generate_oauth_link(user_id: int):
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, GOOGLE_DRIVE_SCOPES)
    flow.redirect_uri = 'http://localhost:8080'
    
    auth_url, _ = flow.authorization_url(prompt='consent')
    
    return auth_url, flow

def get_user_drive_folder(user_id: int) -> tuple[str, str] | None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT folder_id, folder_name FROM google_drive_folders WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return (row[0], row[1]) if row else None

def set_user_drive_folder(user_id: int, folder_id: str, folder_name: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO google_drive_folders (user_id, folder_id, folder_name) VALUES (?, ?, ?)", (user_id, folder_id, folder_name))
    conn.commit()
    conn.close()

async def get_drive_files(service, parent_id='root', page_token=None):
    q_param = f"'{parent_id}' in parents and trashed = false"
    results = service.files().list(
        pageSize=10,
        fields="nextPageToken, files(id, name, mimeType, size, webViewLink, webContentLink, createdTime, modifiedTime)",
        q=q_param,
        pageToken=page_token
    ).execute()
    items = results.get('files', [])
    next_page_token = results.get('nextPageToken')
    return items, next_page_token

async def get_file_details(service, file_id):
    file = service.files().get(
        fileId=file_id,
        fields="id, name, mimeType, size, createdTime, modifiedTime, webViewLink, webContentLink, parents"
    ).execute()
    return file

async def delete_drive_file(service, file_id):
    service.files().delete(fileId=file_id).execute()

async def create_drive_folder(service, folder_name, parent_id='root'):
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    file = service.files().create(body=file_metadata, fields='id, name').execute()
    return file.get('id'), file.get('name')

async def get_drive_folder_contents(service, folder_id, page_token=None):
    return await get_drive_files(service, parent_id=folder_id, page_token=page_token)

async def get_drive_folder_info(service, folder_id):
    return await service.files().get(fileId=folder_id, fields='id, name, parents').execute()

# ——— SCRAPING FUNCTIONS ——————————————————————————————————————
async def search_piratebay(query: str) -> list[dict]:
    url = f"https://thepiratebay.zone/search/{quote_plus(query)}/1/99/0"
    hits: list[dict] = []
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as sess:
            resp = await sess.get(url)
            resp.raise_for_status()
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="searchResult")
        if not table:
            logging.info(f"No search results table found on TPB for '{query}'")
            return []

        for row in table.find_all("tr")[1:11]:
            try:
                title_tag = row.find("div", class_="detName")
                if not title_tag: continue
                title = title_tag.text.strip()

                magnet_tag = row.find("a", href=re.compile(r"^magnet:"))
                if not magnet_tag: continue
                magnet_link = magnet_tag["href"]
                
                desc_tag = row.find("font", class_="detDesc")
                desc = desc_tag.text if desc_tag else ""

                size_match = re.search(r"Size\s+([^,]+)", desc)
                size = size_match.group(1) if size_match else "Unknown"
                
                category_tag = row.find_all("td")
                category = category_tag[0].text.strip() if len(category_tag) > 0 else "Unknown"

                hits.append({
                    "title":        title,
                    "magnet_link":  magnet_link,
                    "size":         size,
                    "category":     category,
                    "source":       "TPB",
                    "torrent_path": None
                })
            except Exception as e:
                logging.warning(f"Error parsing TPB row for '{query}': {e}")
                continue
    except aiohttp.ClientError as e:
        logging.error(f"HTTP client error searching TPB for '{query}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error searching TPB for '{query}': {e}", exc_info=True)
    return hits

async def search_nyaa(query: str, max_results: int = 10) -> list[dict]:
    url = f"https://nyaa.si/?f=0&c=0_0&q={quote_plus(query)}"
    results = []
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as sess:
            resp = await sess.get(url)
            resp.raise_for_status()
            html = await resp.text()

        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.torrent-list tbody tr")
        
        for row in rows[:max_results]:
            try:
                title_tag = row.select_one("td:nth-child(2) a:last-child")
                if not title_tag: continue
                title = title_tag.text.strip()

                magnet_tag = row.select_one("td:nth-child(3) a[href^='magnet:?']")
                if not magnet_tag: continue
                magnet = magnet_tag["href"]
                
                size_tag = row.select_one("td:nth-child(4)")
                size = size_tag.text.strip() if size_tag else "Unknown"
                
                category = "Anime"

                results.append({
                    "title":        title,
                    "magnet_link":  magnet,
                    "size":         size,
                    "category":     category,
                    "source":       "Nyaa",
                    "torrent_path": None
                })
            except Exception as e:
                logging.warning(f"Error parsing Nyaa row for '{query}': {e}")
                continue
    except aiohttp.ClientError as e:
        logging.error(f"HTTP client error searching Nyaa for '{query}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error searching Nyaa for '{query}': {e}", exc_info=True)
    return results

async def search_mejortorrent(query: str) -> list[dict]:
    BASE_URL = "https://www35.mejortorrent.eu"
    SEARCH_URL = f"{BASE_URL}/busqueda"
    hits = []

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as sess:
            resp = await sess.get(SEARCH_URL, params={"q": query})
            resp.raise_for_status()
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            anchors = soup.select("div.flex.flex-row.mb-2 a")[:10]
            
            for a in anchors:
                torrent_path = None
                try:
                    title = a.get_text(strip=True)
                    page_url = urljoin(BASE_URL, a["href"])

                    page_resp = await sess.get(page_url)
                    page_resp.raise_for_status()
                    page_html = await page_resp.text()
                    page_soup = BeautifulSoup(page_html, "html.parser")
                    torrent_link_tag = page_soup.find(
                        "a", href=lambda u: u and u.endswith(".torrent")
                    )
                    if not torrent_link_tag:
                        logging.info(f"No .torrent link found for '{title}' on MejorTorrent.")
                        continue
                    torrent_url = urljoin(BASE_URL, torrent_link_tag["href"])

                    tor_resp = await sess.get(torrent_url)
                    tor_resp.raise_for_status()
                    torrent_data = await tor_resp.read()
                    
                    temp_torrent_file = tempfile.NamedTemporaryFile(delete=False, suffix=".torrent")
                    temp_torrent_file.write(torrent_data)
                    temp_torrent_file.close()
                    torrent_path = temp_torrent_file.name

                    info = lt.torrent_info(torrent_path)
                    
                    total_bytes = info.total_size()
                    size_text = format_bytes(total_bytes)
                    magnet = lt.make_magnet_uri(info)

                    hits.append({
                        "title":        title,
                        "magnet_link":  magnet,
                        "size":         size_text,
                        "category":     "N/A",
                        "source":       "MejorTorrent",
                        "torrent_path": None
                    })
                except aiohttp.ClientError as e:
                    logging.warning(f"HTTP client error processing '{title}' on MejorTorrent: {e}")
                except Exception as e:
                    logging.warning(f"Error parsing or processing torrent from MejorTorrent for '{title}': {e}")
                finally:
                    if torrent_path and os.path.exists(torrent_path):
                        try:
                            os.remove(torrent_path)
                        except OSError as e:
                            logging.warning(f"Could not delete temporary file {torrent_path}: {e}")
    except aiohttp.ClientError as e:
        logging.error(f"HTTP client error searching MejorTorrent for '{query}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error searching MejorTorrent for '{query}': {e}", exc_info=True)
    return hits
    
async def search_gamestorrents(query: str, max_resultados: int = 10) -> list[dict]:
    url = f"https://www.gamestorrents.app/?s={quote_plus(query)}"
    resultados = []

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as sess:
            resp = await sess.get(url)
            resp.raise_for_status()
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            bloques = soup.find_all("div", class_=lambda c: c and "boxedbux" in c)

            for bloque in bloques:
                if len(resultados) >= max_resultados:
                    break
                
                plataforma_tag = bloque.find("span", class_="hometitlen")
                plataforma = (
                    plataforma_tag.get_text(strip=True).replace("Juegos Para", "").strip()
                    if plataforma_tag else "Desconocido"
                )

                tabla = bloque.find("table")
                if not tabla:
                    continue

                filas = tabla.find_all("tr")[1:]
                for fila in filas:
                    if len(resultados) >= max_resultados:
                        break
                    cols = fila.find_all("td")
                    if len(cols) < 5:
                        continue

                    nombre_tag    = cols[0].find("a")
                    if not nombre_tag: continue
                    nombre    = nombre_tag.get_text(strip=True)
                    url_juego = urljoin(url, nombre_tag["href"]) if "href" in nombre_tag.attrs else None
                    
                    tamaño_tag    = cols[2]
                    tamaño    = tamaño_tag.get_text(strip=True) if tamaño_tag else "N/A"
                    torrent_path = None

                    if url_juego:
                        try:
                            jr = await sess.get(url_juego)
                            jr.raise_for_status()
                            jsoup = BeautifulSoup(await jr.text(), "html.parser")
                            at = jsoup.find("a", href=lambda x: x and x.endswith(".torrent") and "/wp-content/uploads/files/" in x)
                            if at and "href" in at.attrs:
                                torrent_url = urljoin(url_juego, at["href"])
                                tf = await sess.get(torrent_url)
                                tf.raise_for_status()
                                contenido = await tf.read()
                                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".torrent")
                                temp_file.write(contenido)
                                temp_file.close()
                                torrent_path = temp_file.name
                            else:
                                logging.info(f"No .torrent link found on game page for '{nombre}'")
                        except aiohttp.ClientError as e:
                            logging.warning(f"HTTP client error downloading .torrent for '{nombre}': {e}")
                        except Exception as e:
                            logging.warning(f"Error processing game page or downloading .torrent for '{nombre}': {e}")

                    if torrent_path:
                        resultados.append({
                            "title":        f"{nombre} [{plataforma}]",
                            "magnet_link":  None,
                            "torrent_path": torrent_path,
                            "size":         tamaño,
                            "category":     plataforma,
                            "source":       "GameTorrents"
                        })
                    else:
                        if torrent_path and os.path.exists(torrent_path):
                            try: os.remove(torrent_path)
                            except OSError as e: logging.warning(f"Could not delete temporary file {torrent_path}: {e}")

        return resultados
    except aiohttp.ClientError as e:
        logging.error(f"HTTP client error searching GameTorrents for '{query}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error searching GameTorrents for '{query}': {e}", exc_info=True)
    return []
    
async def search_yts(query: str, max_resultados: int = 6) -> list[dict]:
    base_url = "https://yts.mx/browse-movies/"
    url = f"{base_url}{quote_plus(query)}"
    resultados = []

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as sess:
            resp = await sess.get(url)
            resp.raise_for_status()
            html = await resp.text()

            soup = BeautifulSoup(html, "html.parser")
            tarjetas = soup.select(".browse-movie-wrap")[:max_resultados]

            for card in tarjetas:
                try:
                    titulo_tag = card.select_one(".browse-movie-title")
                    if not titulo_tag: continue
                    titulo = titulo_tag.text.strip()
                    
                    año_tag = card.select_one(".browse-movie-year")
                    año = año_tag.text.strip() if año_tag else "N/A"
                    
                    link_tag = card.select_one("a")
                    if not link_tag or "href" not in link_tag.attrs: continue
                    link = link_tag["href"]

                    det_resp = await sess.get(link)
                    det_resp.raise_for_status()
                    det_soup = BeautifulSoup(await det_resp.text(), "html.parser")
                    enlaces = det_soup.select("a.download-torrent")

                    magnets = []
                    for a in enlaces:
                        href = a.get("href", "")
                        if not href.startswith("magnet:?"):
                            continue

                        quality_text = a.get_text(strip=True)
                        quality = "Unknown"
                        size = "Unknown"
                        
                        if "720p" in quality_text:
                            quality = "720p"
                        elif "1080p" in quality_text:
                            quality = "1080p"
                        elif "2160p" in quality_text or "4K" in quality_text:
                            quality = "4K"
                        
                        size_match = re.search(r'(\d+(?:\.\d+)?\s*[KMGT]B)', quality_text, re.IGNORECASE)
                        if size_match:
                            size = size_match.group(1)

                        magnets.append({
                            "quality": quality,
                            "size": size,
                            "magnet_link": href
                        })

                    if magnets:
                        resultados.append({
                            "title": f"{titulo} ({año})",
                            "magnets": magnets,
                            "source": "YTS"
                        })
                        
                except aiohttp.ClientError as e:
                    logging.warning(f"HTTP client error processing movie on YTS: {e}")
                    continue
                except Exception as e:
                    logging.warning(f"Error processing movie on YTS: {e}")
                    continue
                    
    except aiohttp.ClientError as e:
        logging.error(f"HTTP client error searching YTS for '{query}': {e}")
    except Exception as e:
        logging.error(f"Unexpected error searching YTS for '{query}': {e}", exc_info=True)
    return resultados

# ——— File Upload Logic —
import os
import zipfile
import asyncio
import aiofiles
import logging
import re
import time # Importar time para calcular ETA y velocidad

# Asumiendo que MAX_TG_FILE y get_text están definidos en el contexto global
# Si no lo están, necesitarás importarlos o definirlos aquí para que este fragmento sea ejecutable de forma independiente.
# Ejemplo (para propósitos de prueba si ejecutas solo este fragmento):
# from pyrogram.types import Message # Necesario para el tipo Message
# MAX_TG_FILE = 2 * 1024**3
# def get_text(uid, key, **kwargs):
#     texts = {
#         "uploading_to_telegram": "📤 Subiendo archivo a Telegram...",
#         "uploading_to_telegram_progress": "📤 {progress:.0f}% — {uploaded_size} / {total_size}\n⚡ {speed}/s\n⏳ ETA: {eta}",
#         "operation_finished": "✅ Operación finalizada.",
#         "file_too_large_telegram": "⚠️ Archivo demasiado grande para subir directamente a Telegram. Dividiendo...",
#         "splitting_and_uploading": "📦 Comprimiendo y dividiendo el archivo...",
#         "uploading_part": "📤 Subiendo parte {current} de {total} a Telegram...",
#         "uploading_part_progress": "📤 Subiendo parte {part_num} de {total_parts} — {progress:.0f}% — {uploaded_size} / {total_size}\n⚡ {speed}/s\n⏳ ETA: {eta}",
#         "calculating_eta": "Calculando...",
#         "not_available_abbr": "N/A"
#     }
#     return texts.get(key, f"MISSING_TEXT: {key}").format(**kwargs)
#
# def format_bytes(size: float) -> str:
#     for unit in ['B','KB','MB','GB','TB']:
#         if size < 1024:
#             return f"{size:.2f}{unit}"
#         size /= 1024
#     return f"{size:.2f}{unit}"
#
# class Message: # Mock de Message para pruebas
#     def __init__(self, text="initial", chat_id=123, from_user_id=1):
#         self.text = text
#         self.chat = type('obj', (object,), {'id': chat_id})()
#         self.from_user = type('obj', (object,), {'id': from_user_id})()
#         self._last_edit_time = 0.0
#         self._last_text = ""
#     async def edit(self, text):
#         print(f"EDITED: {text}")
#         self.text = text
#     async def reply(self, text):
#         print(f"REPLY: {text}")
#     async def send_document(self, chat_id, file_path, caption, progress, progress_args):
#         print(f"SENDING DOCUMENT: {file_path} with caption {caption}")
#         # Simular progreso
#         total_size = os.path.getsize(file_path)
#         for i in range(0, 101, 10):
#             current_size = total_size * i / 100
#             await progress(current_size, total_size, *progress_args)
#             await asyncio.sleep(0.5)
#         print(f"FINISHED SENDING: {file_path}")


def create_zip(file_path: str) -> str:
    zip_path = f"{file_path}.zip"
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(file_path, os.path.basename(file_path))
    return zip_path


async def async_split_file(input_path: str, volume_size: str = "1900M") -> list[str]:
    """
    Comprime el archivo/carpeta en partes usando 7z vía subprocess.
    Conserva el nombre original de la función para integrarlo fácilmente.
    Retorna las rutas de las partes generadas.
    """
    output_path = f"{input_path}.7z"
    cmd = [
        "7z", "a", output_path, input_path,
        f"-v{volume_size}"
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        raise Exception(f"Error al ejecutar 7z:\n{stderr.decode()}")

    # Recolectar las partes generadas
    base_dir = os.path.dirname(output_path)
    base_name = os.path.basename(output_path)
    part_files = sorted([
        os.path.join(base_dir, f) for f in os.listdir(base_dir)
        if f.startswith(base_name) and re.match(r'.*\.7z\.\d{3}$', f)
    ])
    return part_files
    

def make_progress_bar(current: int, total: int, length: int = 12) -> str:
    proportion = current / total
    filled = int(proportion * length)
    empty = length - filled
    percent = proportion * 100
    bar = "█" * filled + "─" * empty
    return f"[{bar}] {percent:5.1f}%"

# MODIFICADO: _progress_callback para incluir velocidad y ETA
async def _progress_callback(
    current: int,
    total: int,
    status_msg: 'Message', # Usar 'Message' para evitar problemas de importación circular si se mueve
    prefix_template: str, # Ahora es una plantilla para el prefijo
    start_time: float,
    uid: int
):
    """
    Callback invocado durante la subida.
    Edita cada 5 segundos y solo si el texto ha cambiado.
    """
    now = time.time()
    
    if not hasattr(status_msg, "_last_edit_time"):
        status_msg._last_edit_time = now
        status_msg._last_text      = ""
        status_msg._last_current   = 0
        status_msg._last_time_for_speed = now

    # Calcular velocidad y ETA
    elapsed_total = now - start_time
    speed = 0
    eta = get_text(uid, "calculating_eta")

    if elapsed_total > 0 and current > 0:
        # Calcular velocidad promedio desde el inicio de la subida
        speed = current / elapsed_total
        
        # Calcular ETA
        remaining_bytes = total - current
        if speed > 0:
            eta_seconds = remaining_bytes / speed
            m, s = divmod(int(eta_seconds), 60)
            eta = f"{m}m {s}s"
        else:
            eta = get_text(uid, "not_available_abbr")

    bar = make_progress_bar(current, total)
    
    # Formatear tamaños y velocidad
    uploaded_size_formatted = format_bytes(current)
    total_size_formatted = format_bytes(total)
    speed_formatted = format_bytes(speed)

    # Construir el nuevo texto usando la plantilla de prefijo
    new_text = prefix_template.format(
        progress=current * 100 / total if total > 0 else 0,
        uploaded_size=uploaded_size_formatted,
        total_size=total_size_formatted,
        speed=speed_formatted,
        eta=eta,
        bar=bar # Pasar la barra de progreso para que la plantilla la use
    )

    if (now - status_msg._last_edit_time >= 5.0
            and new_text != status_msg._last_text):
        try:
            await status_msg.edit(new_text)
            status_msg._last_edit_time = now
            status_msg._last_text      = new_text
            status_msg._last_current   = current
            status_msg._last_time_for_speed = now
        except Exception as e:
            logging.warning(f"Error editing progress message: {e}")


# MODIFICADO: upload_file_to_telegram para usar la nueva _progress_callback
async def upload_file_to_telegram(chat_id: int, status: 'Message', file_path: str, caption: str):
    uid = status.from_user.id
    total_size = os.path.getsize(file_path)
    upload_start_time = time.time() # Registrar el tiempo de inicio de la subida

    async def safe_edit(new_text: str):
        if status.text != new_text:
            await status.edit(new_text)

    # Si el archivo no supera el límite, lo subimos directamente
    if total_size <= MAX_TG_FILE:
        await safe_edit(get_text(uid, "uploading_to_telegram")) # Mensaje inicial
        
        # Plantilla para el progreso de un solo archivo
        prefix_template = get_text(uid, "uploading_to_telegram_progress_single_file")
        
        await bot.send_document(
            chat_id,
            file_path,
            caption=caption,
            progress=_progress_callback,
            progress_args=(status, prefix_template, upload_start_time, uid)
        )
        await safe_edit(get_text(uid, "operation_finished"))
        return

    # Archivo demasiado grande: dividirlo con 7z
    await safe_edit(get_text(uid, "file_too_large_telegram"))
    
    # Mensaje para la compresión
    compression_message = get_text(uid, "splitting_and_uploading")
    await safe_edit(compression_message)

    # Ejecutar la compresión en un hilo separado
    loop = asyncio.get_event_loop()
    try:
        parts = await loop.run_in_executor(
            None,
            lambda: asyncio.run(async_split_file(file_path, volume_size="1900M"))
        )
    except Exception as e:
        logging.error(f"Error during 7z splitting: {e}")
        await safe_edit(get_text(uid, "error_splitting_file", error=e))
        return

    total_parts = len(parts)

    # Subir cada parte individualmente
    for i, part in enumerate(parts):
        part_upload_start_time = time.time() # Tiempo de inicio para cada parte
        
        # Plantilla para el progreso de las partes
        prefix_template = get_text(uid, "uploading_part_progress", part_num=i + 1, total_parts=total_parts)
        
        await safe_edit(get_text(uid, "uploading_part", current=i + 1, total=total_parts)) # Mensaje inicial de la parte
        
        await bot.send_document(
            chat_id,
            part,
            caption=f"{caption} (Parte {i + 1}/{total_parts})",
            progress=_progress_callback,
            progress_args=(status, prefix_template, part_upload_start_time, uid)
        )

    # Eliminar las partes y el archivo comprimido .7z
    for part in parts:
        if os.path.exists(part):
            try:
                os.remove(part)
            except Exception as e:
                logging.warning(f"No se pudo eliminar la parte {part}: {e}")

    compressed_7z = f"{file_path}.7z"
    if os.path.exists(compressed_7z):
        try:
            os.remove(compressed_7z)
        except Exception as e:
            logging.warning(f"No se pudo eliminar el archivo comprimido {compressed_7z}: {e}")

    await safe_edit(get_text(uid, "operation_finished"))



async def upload_file_to_google_drive(user_id: int, status: Message, file_path: str, caption: str):
    service = await get_authenticated_drive_service(user_id, status)
    if not service:
        await status.edit(get_text(user_id, "drive_connection_error"))
        return

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    # Get the user's destination folder
    user_folder_info = get_user_drive_folder(user_id)
    parent_folder_id = user_folder_info[0] if user_folder_info else 'root'

    try:
        await status.edit(get_text(user_id, "uploading_to_drive", file_name=file_name))
        
        file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink' # Added webContentLink for direct download
        )
        response = None
        while response is None:
            status_drive, response = request.next_chunk()
            if status_drive:
                progress_percent = int(status_drive.resumable_progress * 100 / file_size)
                # Only edit if the text has changed to avoid MESSAGE_NOT_MODIFIED
                current_text = get_text(user_id, "uploading_to_drive_progress", file_name=file_name, progress=progress_percent)
                if status.text != current_text:
                    await status.edit(current_text)
                await asyncio.sleep(2)
        
        file_id = response.get('id')
        web_view_link = response.get('webViewLink')
        web_content_link = response.get('webContentLink') # Direct download link

        buttons = [[InlineKeyboardButton(get_text(user_id, "drive_view_on_drive"), url=web_view_link)]]
        if web_content_link:
            buttons.append([InlineKeyboardButton(get_text(user_id, "drive_direct_download"), url=web_content_link)])
        
        await status.edit(get_text(user_id, "drive_upload_success", file_name=file_name, link=web_view_link), reply_markup=InlineKeyboardMarkup(buttons))

    except HttpError as error:
        logging.error(f"Error uploading to Google Drive for {user_id}: {error}")
        if error.resp.status == 401:
            await status.edit(get_text(user_id, "drive_auth_expired"))
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM google_drive_tokens WHERE user_id=?", (user_id,))
            conn.commit()
            conn.close()
        else:
            await status.edit(get_text(user_id, "drive_upload_error", error=error))
    except Exception as e:
        logging.error(f"Unexpected error uploading to Google Drive for {user_id}: {e}", exc_info=True)
        await status.edit(get_text(user_id, "drive_unexpected_error", error=e))
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# ——— Torrent Download and Queue Logic —
async def process_torrent_link_queued(queue_item: dict):
    uid = queue_item['user_id']
    chat_id = queue_item['chat_id']
    magnet = queue_item['magnet_link']
    title = queue_item['title']
    status_message_id = queue_item['status_message_id']
    queue_id = queue_item['queue_id']

    status_msg = None
    try:
        status_msg = await bot.get_messages(chat_id, status_message_id)
        await status_msg.edit(get_text(uid, "download_queue_processing", title=title))
        
        # Update status in DB
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE download_queue SET status='processing' WHERE queue_id=?", (queue_id,))
        conn.commit()
        conn.close()

        download_id = None
        if not validate_magnet_link(magnet):
            await status_msg.edit(get_text(uid, "invalid_magnet_link"))
            raise ValueError("Invalid magnet link")

        ses = None
        handle = None # Initialize handle to None
        try:
            ses = lt.session()
            ses.listen_on(6881, 6891)
            params = {
                "save_path": tempfile.gettempdir(),
                "storage_mode": lt.storage_mode_t.storage_mode_sparse
            }
            handle = lt.add_magnet_uri(ses, magnet, params)

            timeout_meta = 60
            start_meta = time.time()
            await status_msg.edit(get_text(uid, "getting_metadata"))
            while not handle.has_metadata():
                if time.time() - start_meta > timeout_meta:
                    await status_msg.edit(get_text(uid, "metadata_timeout"))
                    ses.remove_torrent(handle)
                    raise TimeoutError("Metadata timeout")
                await asyncio.sleep(1)

            ti = handle.get_torrent_info()
            fname = ti.files().file_path(0).split(os.sep)[-1]
            total_size_bytes = ti.total_size()

            try:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO user_torrents(user_id,title,magnet,size,size_bytes,added_at) VALUES(?,?,?,?,?,?)",
                    (uid, fname, magnet, format_bytes(total_size_bytes), total_size_bytes,
                     datetime.now(timezone.utc).isoformat())
                )
                conn.commit()
            except Exception as e:
                logging.error(f"Error registering torrent in DB: {e}")
            finally:
                conn.close()

            user_name = getattr(status_msg.from_user, "username", None) or "Anonymous"
            download_id = f"{uid}_{int(time.time())}"
            active_downloads[download_id] = {
                "handle": handle,
                "paused": False,
                "cancelled": False,
                "start": time.time(),
                "title": fname,
                "user_id": uid,
                "username": user_name,
                "total_size_bytes": total_size_bytes,
                "id": download_id
            }

            ui_text = render_progress_ui(
                uid, fname, user_name, get_text(uid, "downloading_status"), "0 KB/s", get_text(uid, "calculating_eta")
            )
            buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(uid, "pause_button"), callback_data=f"pause:{download_id}")],
                [InlineKeyboardButton(get_text(uid, "cancel_button"), callback_data=f"cancel:{download_id}")]
            ])
            await status_msg.edit_text(ui_text, reply_markup=buttons)

            download_timeout = 3600
            download_start_time = time.time()

            while handle.status().state != lt.torrent_status.seeding:
                if time.time() - download_start_time > download_timeout:
                    await status_msg.edit_text(get_text(uid, "download_timeout"), reply_markup=None)
                    raise TimeoutError("Download timeout")

                if active_downloads[download_id]["cancelled"]:
                    await status_msg.edit_text(get_text(uid, "download_cancelled_by_user"), reply_markup=None)
                    raise asyncio.CancelledError("Download cancelled by user")

                if active_downloads[download_id]["paused"]:
                    pause_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton(get_text(uid, "resume_button"), callback_data=f"resume:{download_id}")],
                        [InlineKeyboardButton(get_text(uid, "cancel_button"), callback_data=f"cancel:{download_id}")]
                    ])
                    await status_msg.edit_text(
                        render_paused_ui(uid, fname, user_name),
                        reply_markup=pause_kb
                    )
                    while active_downloads[download_id]["paused"] and not active_downloads[download_id]["cancelled"]:
                        await asyncio.sleep(1)
                    if active_downloads[download_id]["cancelled"]:
                        await status_msg.edit_text(get_text(uid, "download_cancelled_by_user"), reply_markup=None)
                        raise asyncio.CancelledError("Download cancelled by user")
                    active_downloads[download_id]["start"] = time.time()
                    # Re-edit the message to show active download UI after resuming
                    await status_msg.edit_text(ui_text, reply_markup=buttons)

                s = handle.status()
                prog = s.progress * 100
                speed = format_bytes(s.download_rate) + "/s"
                eta = calculate_eta(active_downloads[download_id]["start"], total_size_bytes, s.total_done)
                
                new_ui_text = render_progress_ui(
                    uid, fname, user_name, get_text(uid, "downloading_progress", progress=f"{prog:.1f}"), speed, eta
                )
                # Only edit if the text has changed to avoid MESSAGE_NOT_MODIFIED
                if status_msg.text != new_ui_text:
                    await status_msg.edit_text(new_ui_text, reply_markup=buttons)
                await asyncio.sleep(5)

            active_downloads.pop(download_id, None)
            path = os.path.join(params["save_path"], ti.files().file_path(0))
            
            if os.path.exists(path):
                upload_mode = get_user_upload_mode(uid)
                if upload_mode == 'telegram':
                    await upload_file_to_telegram(chat_id, status_msg, path, get_text(uid, "completed_download", file_name=fname))
                elif upload_mode == 'drive':
                    await upload_file_to_google_drive(uid, status_msg, path, get_text(uid, "completed_download", file_name=fname))
            else:
                await status_msg.edit(get_text(uid, "file_not_found_after_download"), reply_markup=None)
            
            # Update status in DB
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("UPDATE download_queue SET status='completed' WHERE queue_id=?", (queue_id,))
            conn.commit()
            conn.close()

        except (TimeoutError, asyncio.CancelledError) as e:
            logging.info(f"Download {download_id} for user {uid} ended: {e}")
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("UPDATE download_queue SET status='cancelled' WHERE queue_id=?", (queue_id,))
            conn.commit()
            conn.close()
            if download_id and download_id in active_downloads:
                active_downloads.pop(download_id, None)
            if ses and handle:
                try: ses.remove_torrent(handle)
                except Exception as cleanup_e: logging.warning(f"Error cleaning up torrent after failure: {cleanup_e}")
        except Exception as e:
            logging.error(f"Error in process_torrent_link_queued for magnet {magnet}: {e}", exc_info=True)
            if status_msg:
                await status_msg.edit(get_text(uid, "error_downloading", error=e), reply_markup=None)
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("UPDATE download_queue SET status='failed' WHERE queue_id=?", (queue_id,))
            conn.commit()
            conn.close()
            if download_id and download_id in active_downloads:
                active_downloads.pop(download_id, None)
            if ses and handle:
                try: ses.remove_torrent(handle)
                except Exception as cleanup_e: logging.warning(f"Error cleaning up torrent after failure: {cleanup_e}")
    finally:
        if queue_id in processing_downloads:
            del processing_downloads[queue_id]
        # Ensure the worker can take the next task
        asyncio.create_task(download_worker())

async def add_to_download_queue(user_id: int, chat_id: int, magnet_link: str, title: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO download_queue (user_id, chat_id, magnet_link, title, added_at, status) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, chat_id, magnet_link, title, datetime.now(timezone.utc).isoformat(), 'pending')
    )
    queue_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Create an initial status message that will be updated
    status_msg = await bot.send_message(chat_id, get_text(user_id, "download_added_to_queue", title=title))
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE download_queue SET status_message_id=? WHERE queue_id=?", (status_msg.id, queue_id))
    conn.commit()
    conn.close()

    queue_item = {
        'queue_id': queue_id,
        'user_id': user_id,
        'chat_id': chat_id,
        'magnet_link': magnet_link,
        'title': title,
        'status_message_id': status_msg.id
    }
    await download_queue.put(queue_item)
    await status_msg.edit(get_text(user_id, "download_added_to_queue_position", title=title, position=download_queue.qsize()))

async def download_worker():
    while True:
        # Wait if there are too many ongoing downloads
        while len(processing_downloads) >= MAX_SIMULTANEOUS_DOWNLOADS:
            await asyncio.sleep(5) # Wait before retrying

        queue_item = await download_queue.get()
        queue_id = queue_item['queue_id']

        # Check if the task is already being processed or cancelled
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT status FROM download_queue WHERE queue_id=?", (queue_id,))
        status = cur.fetchone()[0]
        conn.close()

        if status in ['completed', 'failed', 'cancelled', 'processing']:
            logging.info(f"Skipping queue item {queue_id} as its status is {status}.")
            download_queue.task_done()
            continue

        logging.info(f"Starting download for queue item {queue_id}")
        task = asyncio.create_task(process_torrent_link_queued(queue_item))
        processing_downloads[queue_id] = task
        download_queue.task_done()

# Start workers when the bot starts
async def start_download_workers():
    for _ in range(MAX_SIMULTANEOUS_DOWNLOADS):
        asyncio.create_task(download_worker())
    logging.info(f"Started {MAX_SIMULTANEOUS_DOWNLOADS} download workers.")

# ——— CUSTOM FILTERS ——————————————————————————————————————
async def is_user_blocked(_, client, update):
    uid = update.from_user.id
    if is_blocked(uid):
        if isinstance(update, CallbackQuery):
            await update.answer(get_text(uid, "blocked_message"), show_alert=True)
        elif isinstance(update, Message):
            await update.reply(get_text(uid, "blocked_message"))
        return False
    return True

async def is_not_maintenance_mode(_, client, update):
    uid = update.from_user.id
    if MAINTENANCE_MODE and not is_admin(uid):
        if isinstance(update, CallbackQuery):
            await update.answer(get_text(uid, "maintenance_message"), show_alert=True)
        elif isinstance(update, Message):
            await update.reply(get_text(uid, "maintenance_message"))
        return False
    return True

async def is_search_enabled(_, client, update):
    uid = update.from_user.id
    if SEARCH_DISABLED and not is_admin(uid):
        if isinstance(update, CallbackQuery):
            await update.answer(get_text(uid, "search_disabled_message"), show_alert=True)
        elif isinstance(update, Message):
            await update.reply(get_text(uid, "search_disabled_message"))
        return False
    return True

# Combine filters for general use
user_allowed_filter = filters.create(is_user_blocked) & filters.create(is_not_maintenance_mode)
search_allowed_filter = filters.create(is_user_blocked) & filters.create(is_not_maintenance_mode) & filters.create(is_search_enabled)

# ——— COMMAND AND CALLBACK HANDLING ——————————————————————————————
@bot.on_message(filters.command("start") & filters.private & user_allowed_filter)
async def on_start(_, m: Message):
    uid = m.from_user.id
    logging.info(f"User {uid} started the bot.")
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    cur.execute("SELECT is_new_user FROM users WHERE user_id=?", (uid,))
    user_data = cur.fetchone()
    
    is_new_user = True
    if user_data:
        is_new_user = user_data[0] == 1
    
    if is_new_user:
        cur.execute(
            "INSERT OR IGNORE INTO users(user_id,username,joined_at,is_new_user,upload_mode,language_code) VALUES(?,?,?,?,?,?)",
            (uid, m.from_user.username or m.from_user.first_name,
             datetime.now(timezone.utc).isoformat(), False, 'telegram', DEFAULT_LANG)
        )
        cur.execute("UPDATE users SET is_new_user = FALSE WHERE user_id = ?",(uid,))
        conn.commit()
        welcome_message = get_text(uid, "welcome_new_user", first_name=m.from_user.first_name)
    else:
        welcome_message = get_text(uid, "welcome_back_user", first_name=m.from_user.first_name)
    
    conn.close()
    
    header, menu = build_main_menu(uid)
    await m.reply(f"{welcome_message}\n\n{header}", reply_markup=menu)

@bot.on_callback_query(filters.regex(r"^menu:") & user_allowed_filter)
async def menu_handler(_, q: CallbackQuery):
    action = q.data.split(":",2)[1]
    uid = q.from_user.id

    if action == "search":
        await q.message.edit(get_text(uid, "search_usage"))
        return await q.answer()

    if action == "downloads":
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
          SELECT title,size,added_at FROM user_torrents
          WHERE user_id=? ORDER BY added_at DESC LIMIT 5
        """, (uid,))
        rows = cur.fetchall()
        conn.close()
        txt = get_text(uid, "downloads_recent_title") + "\n"
        if not rows:
            txt += get_text(uid, "no_downloads_found")
        else:
            for t,s,a in rows:
                d = datetime.fromisoformat(a).strftime("%d/%m/%Y")
                txt += f"• {t} ({s}) – {d}\n"
        txt += f"\n{separator()}"
        header, menu = build_main_menu(uid)
        
        # Only edit if the text or markup has changed
        new_text = f"{txt}\n{header}"
        new_markup = menu
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        return await q.answer()

    if action == "settings":
        header = get_profile_header(uid)
        new_text = f"{get_text(uid, 'settings_title')}\n{header}"
        new_markup = build_settings_menu(uid)
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        return await q.answer()

    if action == "help":
        header = get_profile_header(uid)
        new_text = f"{get_text(uid, 'help_title')}\n{header}"
        new_markup = build_help_menu(uid)
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        return await q.answer()

    if action == "exit":
        new_text = get_text(uid, "goodbye_message")
        if q.message.text != new_text:
            await q.message.edit(new_text)
        return await q.answer()

    if action == "back_main":
        header, menu = build_main_menu(uid)
        new_text = f"{header}"
        new_markup = menu
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        return await q.answer()

# --- Settings Callbacks ---
@bot.on_callback_query(filters.regex(r"^settings:toggle_theme$") & user_allowed_filter)
async def settings_toggle_theme_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    await q.answer(get_text(uid, "theme_toggle_not_implemented"), show_alert=True)
    header = get_profile_header(uid)
    new_text = f"{get_text(uid, 'settings_title')}\n{header}"
    new_markup = build_settings_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)

@bot.on_callback_query(filters.regex(r"^settings:notifications$") & user_allowed_filter)
async def settings_notifications_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    await q.answer(get_text(uid, "notifications_not_implemented"), show_alert=True)
    header = get_profile_header(uid)
    new_text = f"{get_text(uid, 'settings_title')}\n{header}"
    new_markup = build_settings_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)

@bot.on_callback_query(filters.regex(r"^settings:clear_history$") & user_allowed_filter)
async def settings_clear_history_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    await q.answer(get_text(uid, "clear_history_not_implemented"), show_alert=True)
    header = get_profile_header(uid)
    new_text = f"{get_text(uid, 'settings_title')}\n{header}"
    new_markup = build_settings_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)

@bot.on_callback_query(filters.regex(r"^settings:upload_mode$") & user_allowed_filter)
async def settings_upload_mode_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    new_text = get_text(uid, "choose_upload_mode")
    new_markup = build_upload_mode_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)
    await q.answer()

@bot.on_callback_query(filters.regex(r"^settings:set_upload_mode:") & user_allowed_filter)
async def settings_set_upload_mode_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    new_mode = q.data.split(":")[2]
    set_user_upload_mode(uid, new_mode)
    await q.answer(get_text(uid, "upload_mode_changed", mode=new_mode.capitalize()), show_alert=True)
    new_text = get_text(uid, "choose_upload_mode")
    new_markup = build_upload_mode_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)

@bot.on_callback_query(filters.regex(r"^settings:language$") & user_allowed_filter)
async def settings_language_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    new_text = get_text(uid, "choose_language")
    new_markup = build_language_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)
    await q.answer()

@bot.on_callback_query(filters.regex(r"^settings:set_language:") & user_allowed_filter)
async def settings_set_language_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    new_lang_code = q.data.split(":")[2]
    set_user_language(uid, new_lang_code)
    header = get_profile_header(uid)
    new_text = f"{get_text(uid, 'settings_title')}\n{header}"
    new_markup = build_settings_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)
    await q.answer(get_text(uid, "language_changed", lang=AVAILABLE_LANGS.get(new_lang_code, new_lang_code)), show_alert=True)

# --- Google Drive Commands & Callbacks ---
@bot.on_message(filters.command("drive") & filters.private & user_allowed_filter)
async def drive_command(_, m: Message):
    uid = m.from_user.id
    
    creds = get_google_drive_credentials(uid)
    if not creds or not creds.valid:
        auth_url, flow = await generate_oauth_link(uid)
        user_search_states[uid] = {"drive_flow": flow, "awaiting_drive_code": False} 
        
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "drive_access_link"), url=auth_url)],
            [InlineKeyboardButton(get_text(uid, "drive_got_code"), callback_data="drive:auth_code")]
        ])
        await m.reply(
            get_text(uid, "drive_auth_needed"),
            reply_markup=buttons
        )
    else:
        await show_drive_menu(m, uid)

async def show_drive_menu(message: Message, user_id: int, current_folder_id: str = 'root', page_token: str = None):
    service = await get_authenticated_drive_service(user_id, message)
    if not service:
        return

    try:
        files, next_page_token = await get_drive_files(service, parent_id=current_folder_id, page_token=page_token)
        
        text = ""
        buttons = []
        
        # Show current folder name and button to go to parent folder
        current_folder_name = get_text(user_id, "drive_root_folder")
        parent_folder_id = None
        if current_folder_id != 'root':
            try:
                folder_info = await get_drive_folder_info(service, current_folder_id)
                current_folder_name = folder_info.get('name', 'Unknown Folder')
                parents = folder_info.get('parents')
                if parents:
                    parent_folder_id = parents[0] # Assume a single parent for simplicity
            except HttpError as e:
                logging.warning(f"Could not get info for folder {current_folder_id}: {e}")
                current_folder_name = get_text(user_id, "drive_folder_error")

        text += f"📂 {get_text(user_id, 'drive_current_folder')}: `{current_folder_name}`\n\n"
        
        if not files:
            text += get_text(user_id, "drive_no_files")
        else:
            for i, file in enumerate(files):
                file_name = file['name']
                file_size = format_bytes(int(file['size'])) if 'size' in file else get_text(user_id, "folder_text")
                
                if file['mimeType'] == 'application/vnd.google-apps.folder':
                    text += f"📁 {file_name}/\n"
                    buttons.append([InlineKeyboardButton(f"📂 {file_name}", callback_data=f"drive:open_folder:{file['id']}:{current_folder_id}")])
                else:
                    text += f"📄 {file_name} ({file_size})\n"
                    buttons.append([InlineKeyboardButton(f"ℹ️ {file_name}", callback_data=f"drive:detail:{file['id']}:{current_folder_id}:{page_token or 'none'}")])
        
        nav_buttons = []
        if parent_folder_id:
            nav_buttons.append(InlineKeyboardButton(get_text(user_id, "drive_parent_folder"), callback_data=f"drive:open_folder:{parent_folder_id}:{current_folder_id}"))
        if page_token:
            nav_buttons.append(InlineKeyboardButton(get_text(user_id, "drive_prev_page"), callback_data=f"drive:page:{current_folder_id}:prev"))
        if next_page_token:
            nav_buttons.append(InlineKeyboardButton(get_text(user_id, "drive_next_page"), callback_data=f"drive:page:{current_folder_id}:{next_page_token}"))
        
        if nav_buttons: # Only append if there are navigation buttons
            buttons.append(nav_buttons)
        buttons.append([InlineKeyboardButton(get_text(user_id, "drive_set_upload_folder"), callback_data=f"drive:set_upload_folder:{current_folder_id}:{current_folder_name}")])
        buttons.append([InlineKeyboardButton(get_text(user_id, "drive_create_folder"), callback_data=f"drive:create_folder:{current_folder_id}")])
        buttons.append([InlineKeyboardButton(get_text(user_id, "drive_refresh"), callback_data=f"drive:menu:{current_folder_id}")])
        buttons.append([InlineKeyboardButton(get_text(user_id, "drive_disconnect"), callback_data="drive:disconnect")])

        new_markup = InlineKeyboardMarkup(buttons)
        if message.text != text or message.reply_markup != new_markup:
            await message.edit_text(text, reply_markup=new_markup)

    except HttpError as error:
        logging.error(f"Error listing Drive files for {user_id}: {error}")
        if error.resp.status == 401:
            await message.reply(get_text(user_id, "drive_auth_expired"))
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM google_drive_tokens WHERE user_id=?", (user_id,))
            conn.commit()
            conn.close()
        else:
            await message.reply(get_text(user_id, "drive_list_error", error=error))
    except Exception as e:
        logging.error(f"Unexpected error showing Drive menu for {user_id}: {e}")
        await message.reply(get_text(user_id, "drive_unexpected_error_menu", error=e))

@bot.on_callback_query(filters.regex(r"^drive:") & user_allowed_filter)
async def drive_callback_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    action_parts = q.data.split(":")
    action = action_parts[1]

    if action == "auth_code":
        if uid not in user_search_states:
            user_search_states[uid] = {}
        user_search_states[uid]["awaiting_drive_code"] = True
        new_text = get_text(uid, "drive_enter_auth_code")
        if q.message.text != new_text:
            await q.message.edit(new_text)
        await q.answer()
        return

    if action == "menu":
        current_folder_id = action_parts[2] if len(action_parts) > 2 else 'root'
        await show_drive_menu(q.message, uid, current_folder_id=current_folder_id)
        await q.answer()
        return

    if action == "page":
        current_folder_id = action_parts[2]
        page_token = action_parts[3] if action_parts[3] != 'prev' else None
        await show_drive_menu(q.message, uid, current_folder_id=current_folder_id, page_token=page_token)
        await q.answer()
        return

    if action == "open_folder":
        folder_id = action_parts[2]
        await show_drive_menu(q.message, uid, current_folder_id=folder_id)
        await q.answer()
        return

    if action == "set_upload_folder":
        folder_id = action_parts[2]
        folder_name = action_parts[3]
        set_user_drive_folder(uid, folder_id, folder_name)
        await q.answer(get_text(uid, "drive_upload_folder_set", folder_name=folder_name), show_alert=True)
        await show_drive_menu(q.message, uid, current_folder_id=folder_id)
        return

    if action == "create_folder":
        parent_folder_id = action_parts[2]
        user_search_states[uid] = {"awaiting_folder_name": True, "parent_folder_id": parent_folder_id}
        new_text = get_text(uid, "drive_enter_folder_name")
        if q.message.text != new_text:
            await q.message.edit(new_text)
        await q.answer()
        return

    if action == "detail":
        file_id = action_parts[2]
        current_folder_id = action_parts[3]
        current_page_token = action_parts[4] if len(action_parts) > 4 and action_parts[4] != 'none' else None
        service = await get_authenticated_drive_service(uid, q.message)
        if not service: return await q.answer(get_text(uid, "auth_error"), show_alert=True)

        try:
            file_details = await get_file_details(service, file_id)
            
            name = file_details.get('name', get_text(uid, 'not_available_abbr'))
            mime_type = file_details.get('mimeType', get_text(uid, 'not_available_abbr'))
            size = format_bytes(int(file_details['size'])) if 'size' in file_details else get_text(uid, 'not_available_abbr')
            created_time = datetime.fromisoformat(file_details['createdTime'].replace('Z', '+00:00')).strftime("%d/%m/%Y %H:%M")
            modified_time = datetime.fromisoformat(file_details['modifiedTime'].replace('Z', '+00:00')).strftime("%d/%m/%Y %H:%M")
            web_view_link = file_details.get('webViewLink', get_text(uid, 'not_available_abbr'))
            web_content_link = file_details.get('webContentLink', get_text(uid, 'not_available_abbr'))

            text = (
                f"{get_text(uid, 'drive_file_details_title')}\n"
                f"{get_text(uid, 'drive_file_name', name=name)}\n"
                f"{get_text(uid, 'drive_file_type', type=mime_type)}\n"
                f"{get_text(uid, 'drive_file_size', size=size)}\n"
                f"{get_text(uid, 'drive_file_created', created=created_time)}\n"
                f"{get_text(uid, 'drive_file_modified', modified=modified_time)}\n"
            )
            
            buttons = [
                [InlineKeyboardButton(get_text(uid, "drive_view_on_drive"), url=web_view_link)]
            ]
            if web_content_link and not mime_type.startswith("application/vnd.google-apps"):
                buttons.append([InlineKeyboardButton(get_text(uid, "drive_direct_download"), url=web_content_link)])
            
            buttons.append([InlineKeyboardButton(get_text(uid, "drive_delete_file"), callback_data=f"drive:delete:{file_id}:{current_folder_id}:{current_page_token or 'none'}")])
            buttons.append([InlineKeyboardButton(get_text(uid, "drive_back_to_files"), callback_data=f"drive:menu:{current_folder_id}")])

            new_markup = InlineKeyboardMarkup(buttons)
            if q.message.text != text or q.message.reply_markup != new_markup:
                await q.message.edit_text(text, reply_markup=new_markup)

        except HttpError as error:
            logging.error(f"Error getting file details {file_id} for {uid}: {error}")
            await q.message.reply(get_text(uid, "drive_error_getting_details", error=error))
        except Exception as e:
            logging.error(f"Unexpected error getting file details {file_id} for {uid}: {e}")
            await q.message.reply(get_text(uid, "drive_unexpected_error_generic", error=e))
        await q.answer()
        return

    if action == "delete":
        file_id = action_parts[2]
        current_folder_id = action_parts[3]
        current_page_token = action_parts[4] if len(action_parts) > 4 and action_parts[4] != 'none' else None
        service = await get_authenticated_drive_service(uid, q.message)
        if not service: return await q.answer(get_text(uid, "auth_error"), show_alert=True)

        try:
            await delete_drive_file(service, file_id)
            await q.answer(get_text(uid, "drive_file_deleted"), show_alert=True)
            await show_drive_menu(q.message, uid, current_folder_id, current_page_token)
        except HttpError as error:
            logging.error(f"Error deleting file {file_id} for {uid}: {error}")
            await q.answer(get_text(uid, "drive_error_deleting_file", error=error), show_alert=True)
        except Exception as e:
            logging.error(f"Unexpected error deleting file {file_id} for {uid}: {e}")
            await q.answer(get_text(uid, "drive_unexpected_error_generic", error=e), show_alert=True)
        return

    if action == "disconnect":
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM google_drive_tokens WHERE user_id=?", (uid,))
        cur.execute("DELETE FROM google_drive_folders WHERE user_id=?", (uid,)) # Also delete default folder
        conn.commit()
        conn.close()
        new_text = get_text(uid, "drive_disconnect_success")
        if q.message.text != new_text:
            await q.message.edit(new_text)
        await q.answer(get_text(uid, "drive_account_disconnected"))
        return
        
        
        
@bot.on_message(filters.command("feedback") & filters.private & user_allowed_filter)
async def feedback_command(_, m: Message):
    uid = m.from_user.id
    
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.reply(get_text(uid, "feedback_usage"))
    
    feedback_text = parts[1].strip()
    user_info = f"@{m.from_user.username}" if m.from_user.username else f"User ID: {uid}"
    
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, get_text(admin_id, "new_feedback", user_info=user_info, message=feedback_text))
        except Exception as e:
            logging.error(f"Error sending feedback to admin {admin_id}: {e}")
    
    await m.reply(get_text(uid, "feedback_sent"))

@bot.on_message(filters.command("stats") & filters.private & user_allowed_filter)
async def stats_command(_, m: Message):
    uid = m.from_user.id
    
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM users");        total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM user_torrents");total_torrents = cur.fetchone()[0]
    cur.execute("SELECT SUM(size_bytes) FROM user_torrents");total_gb_downloaded_bytes = cur.fetchone()[0] or 0
    conn.close()

    total_gb_downloaded = format_bytes(total_gb_downloaded_bytes)

    total_download_rate = 0
    total_upload_rate = 0
    for dl_id in active_downloads:
        dl = active_downloads[dl_id]
        s = dl["handle"].status()
        total_download_rate += s.download_rate
        total_upload_rate += s.upload_rate
    
    text = (
        f"{get_text(uid, 'bot_stats_title')}\n\n"
        f"{get_text(uid, 'admin_total_users', count=total_users)}\n"
        f"{get_text(uid, 'admin_total_torrents', count=total_torrents)}\n"
        f"{get_text(uid, 'admin_total_downloaded_gb', size=total_gb_downloaded)}\n"
        f"{get_text(uid, 'admin_current_bandwidth')}\n"
        f"  {get_text(uid, 'admin_download_rate', speed=format_bytes(total_download_rate))}/s\n"
        f"  {get_text(uid, 'admin_upload_rate', speed=format_bytes(total_upload_rate))}/s"
    )
    await m.reply(text)

import uuid
import sqlite3
import asyncio
import logging
from pyrogram import filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

# Caché temporal de mensajes de difusión
broadcast_temp_messages = {}
broadcast_state = {}

@bot.on_message(filters.command("broadcast") & filters.private & filters.user(ADMIN_IDS))
async def broadcast_command(_, m: Message):
    uid = m.from_user.id
    parts = m.text.split(maxsplit=1)

    if len(parts) < 2:
        broadcast_state[uid] = "pending_message"
        return await m.reply(get_text(uid, "broadcast_usage"))
    
    message_to_send = parts[1].strip()
    msg_id = str(uuid.uuid4())[:8]
    broadcast_temp_messages[msg_id] = message_to_send
    await send_broadcast_confirmation(m.chat.id, msg_id, uid)

async def send_broadcast_confirmation(chat_id: int, msg_id: str, admin_uid: int):
    message_preview = broadcast_temp_messages.get(msg_id)
    if not message_preview:
        return await bot.send_message(chat_id, "⚠️ Error: El mensaje no está disponible.")

    buttons = InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text(admin_uid, "confirm_send"), callback_data=f"broadcast:confirm:{msg_id}")],
        [InlineKeyboardButton(get_text(admin_uid, "cancel_button"), callback_data="broadcast:cancel")]
    ])
    await bot.send_message(
        chat_id,
        f"{get_text(admin_uid, 'confirm_broadcast_title')}\n\n{get_text(admin_uid, 'confirm_broadcast_message', message=message_preview)}",
        reply_markup=buttons
    )

@bot.on_callback_query(filters.regex(r"^broadcast:") & filters.user(ADMIN_IDS))
async def broadcast_callback_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    parts = q.data.split(":")
    action = parts[1]

    if action == "confirm":
        if len(parts) < 3:
            return await q.answer("⚠️ Datos inválidos.", show_alert=True)

        msg_id = parts[2]
        message_to_send = broadcast_temp_messages.pop(msg_id, None)
        if not message_to_send:
            return await q.answer("⚠️ Mensaje no encontrado o expirado.", show_alert=True)

        new_text = get_text(uid, "broadcast_sending")
        if q.message.text != new_text:
            await q.message.edit_text(new_text)

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users")
        all_users = cur.fetchall()
        conn.close()

        sent_count, failed_count = 0, 0

        for (target_uid,) in all_users:
            if target_uid == uid:
                continue
            try:
                await bot.send_message(target_uid, message_to_send)
                sent_count += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                failed_count += 1
                logging.warning(f"Failed to send to {target_uid}: {e}")

        final_text = get_text(uid, "broadcast_finished", sent=sent_count, failed=failed_count)
        if q.message.text != final_text:
            await q.message.edit_text(final_text)
        broadcast_state.pop(uid, None)
        await q.answer(f"✅ {sent_count} enviados, {failed_count} fallidos", show_alert=True)

    elif action == "cancel":
        broadcast_temp_messages.clear()
        cancel_text = get_text(uid, "broadcast_cancelled")
        if q.message.text != cancel_text:
            await q.message.edit_text(cancel_text)
        broadcast_state.pop(uid, None)
        await q.answer(get_text(uid, "broadcast_cancelled_alert"), show_alert=True)

import urllib.parse
from pyrogram import filters
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton



# ----------------------------
# 1) Comando /shop — menú principal
# ----------------------------
import sqlite3
import urllib.parse

from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)


# Estados en curso para el flujo admin
user_shop_states = {}



# ----------------------------
# 0) Helpers de Base de Datos
# ----------------------------
def save_category(name: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO shop_categories(name) VALUES(?)", (name,))
    conn.commit()
    conn.close()

def delete_category(cat_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM shop_categories WHERE id = ?", (cat_id,))
    conn.commit()
    conn.close()

def get_classifications() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM shop_categories")
    rows = cur.fetchall()
    conn.close()
    return [{"id": row[0], "name": row[1]} for row in rows]

def save_product(data: dict):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO shop_products(
            name, desc, price,
            admin_id, admin_username,
            custom_msg, category_id,
            image_file_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["name"], data["desc"], data["price"],
        data["admin_id"], data["admin_username"],
        data["custom_msg"], data["category_id"],
        data.get("image_file_id")
    ))
    conn.commit()
    conn.close()

def update_product_field(prod_id: int, field: str, value):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"UPDATE shop_products SET {field} = ? WHERE id = ?", (value, prod_id))
    conn.commit()
    conn.close()

def delete_product(prod_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM shop_products WHERE id = ?", (prod_id,))
    conn.commit()
    conn.close()
def get_products_by_category(cat_id: int) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM shop_products WHERE category_id = ?", (cat_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_products_by_admin(admin_id: int) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM shop_products WHERE admin_id = ?", (admin_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_product(prod_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM shop_products WHERE id = ?", (prod_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else {}

# ----------------------------
# 1) Comando /shop
# ----------------------------
@bot.on_message(filters.command("shop") & filters.private & user_allowed_filter)
async def shop_command(_, m: Message):
    uid = m.from_user.id
    parts = m.text.split(maxsplit=1)

    # Panel ADMIN
    if len(parts) > 1 and parts[1].lower() == "admin" and uid in ADMIN_IDS:
        text = "🛠️ Panel de Administración de la Tienda\n\n🔹 Gestiona tus ofertas desde aquí:"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Añadir Clasificación", callback_data="shop:add_category")],
            [InlineKeyboardButton("📦 Añadir Producto",     callback_data="shop:add_product")],
            [InlineKeyboardButton("📝 Editar Producto",     callback_data="shop:edit_product")],
            [InlineKeyboardButton("❌ Eliminar Producto",   callback_data="shop:delete_product")],
            [InlineKeyboardButton("🗑️ Eliminar Clasificación", callback_data="shop:delete_category")],
        ])
        return await m.reply(text, reply_markup=markup)

    # Menú USUARIO
    categories = get_classifications()
    if not categories:
        return await m.reply("🚫 No se ha publicado ninguna oferta en la tienda por el momento.")

    text = "🛍️ Tienda Virtual — Clasificaciones Disponibles:\n\n"
    buttons = []
    for cat in categories:
        text += f"📌 {cat['name']}\n"
        buttons.append([InlineKeyboardButton(cat["name"], callback_data=f"shop:category:{cat['id']}")])

    await m.reply(text, reply_markup=InlineKeyboardMarkup(buttons))


# ----------------------------
# 2) Mostrar productos por categoría
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:category:(\d+)$") & user_allowed_filter)
async def shop_category_cb(_, q: CallbackQuery):
    cat_id = int(q.data.split(":")[2])
    products = get_products_by_category(cat_id)

    if not products:
        await q.answer()
        return await q.message.reply("🚫 No hay productos en esta categoría aún.")

    for p in products:
        caption = (
            f"🛒 *{p['name']}*\n\n"
            f"{p['desc']}\n\n"
            f"💲 Precio: {p['price']}\n"
        )
        btn = None
        if p.get("custom_msg"):
            msg_enc = urllib.parse.quote(p["custom_msg"])
            if p["admin_username"]:
                url = f"https://t.me/{p['admin_username']}?text={msg_enc}"
            else:
                url = f"https://t.me/share/url?url=tg://user?id={p['admin_id']}&text={msg_enc}"
            btn = InlineKeyboardButton("🛍️ Comprar", url=url)
        else:
            url = f"https://t.me/share/url?url=tg://user?id={p['admin_id']}"
            btn = InlineKeyboardButton("🛍️ Comprar", url=url)

        markup = InlineKeyboardMarkup([[btn]])

        if p.get("image_file_id"):
            await q.message.reply_photo(
                photo=p["image_file_id"],
                caption=caption,
                reply_markup=markup,
                parse_mode=enums.ParseMode.MARKDOWN
            )
        else:
            await q.message.reply(
                caption,
                reply_markup=markup,
                parse_mode=enums.ParseMode.MARKDOWN
            )

    await q.answer()


# ----------------------------
# 3) Añadir Clasificación
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:add_category$") & filters.user(ADMIN_IDS))
async def shop_add_category_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    user_shop_states[uid] = {"action": "add_category", "step": "name"}
    await q.message.reply("➕ *Añadir Clasificación*\n\nEscribe el *nombre* de la nueva clasificación:")
    await q.answer()


# ----------------------------
# 4) Añadir Producto (inicia flujo)
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:add_product$") & filters.user(ADMIN_IDS))
async def shop_add_product_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    user_shop_states[uid] = {"action": "add_product", "step": "name"}
    await q.message.reply("📦 *Añadir Nuevo Producto*\n\nPor favor envía el *nombre* del producto:")
    await q.answer()


# ----------------------------
# 5.1) Selección de Categoría en Añadir Producto
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:select_cat:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_select_cat_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    state = user_shop_states.get(uid)

    # Verificar que estamos en el flujo correcto
    if not state or state.get("action") != "add_product" or state.get("step") != "category":
        await q.answer()
        return

    # Guardamos la categoría y avanzamos al siguiente paso
    cat_id = int(q.data.split(":")[2])
    state["category_id"] = cat_id
    state["step"] = "custom_msg"

    await q.message.reply(
        "✉️ ¿Quieres añadir un mensaje personalizado para el botón?\n"
        "Envía el texto o escribe `no` para omitir:"
    )
    await q.answer()


# ----------------------------
# 5) Editar Producto (inicia flujo)
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:edit_product$") & filters.user(ADMIN_IDS))
async def shop_edit_product_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    prods = get_products_by_admin(uid)
    if not prods:
        await q.answer()
        return await q.message.reply("🚫 No tienes productos publicados aún.")

    buttons = [
        [InlineKeyboardButton(p["name"], callback_data=f"shop:edit_product_select:{p['id']}")]
        for p in prods
    ]
    await q.message.reply("📝 Elige el producto a editar:", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


@bot.on_callback_query(filters.regex(r"^shop:edit_product_select:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_edit_product_select_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    prod_id = int(q.data.split(":")[2])
    user_shop_states[uid] = {
        "action": "edit_product",
        "prod_id": prod_id,
        "step": "choose_field"
    }
    buttons = [
        [InlineKeyboardButton("Nombre",       callback_data="shop:edit_field:name")],
        [InlineKeyboardButton("Imagen",       callback_data="shop:edit_field:image_file_id")],
        [InlineKeyboardButton("Descripción",  callback_data="shop:edit_field:desc")],
        [InlineKeyboardButton("Precio",       callback_data="shop:edit_field:price")],
        [InlineKeyboardButton("Categoría",    callback_data="shop:edit_field:category_id")],
        [InlineKeyboardButton("Mensaje Botón",callback_data="shop:edit_field:custom_msg")],
        [InlineKeyboardButton("Cancelar",     callback_data="shop:cancel")]
    ]
    await q.message.reply("🔀 ¿Qué campo quieres editar?", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


@bot.on_callback_query(filters.regex(r"^shop:edit_field:(.+)$") & filters.user(ADMIN_IDS))
async def shop_edit_field_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    state = user_shop_states.get(uid)
    field = q.data.split(":")[2]
    state["field"] = field

    if field == "image_file_id":
        prompt = "📷 Envía la *nueva imagen* o escribe `no` para eliminarla:"
    elif field == "category_id":
        cats = get_classifications()
        buttons = [
            [InlineKeyboardButton(c["name"], callback_data=f"shop:edit_value:{c['id']}")]
            for c in cats
        ]
        await q.message.reply("📂 Elige la *nueva categoría*:", reply_markup=InlineKeyboardMarkup(buttons))
        await q.answer()
        return
    else:
        prompt = f"✏️ Envía el *nuevo valor* para el campo {field}:"

    state["step"] = "edit_value"
    await q.message.reply(prompt)
    await q.answer()


# ----------------------------
# 6) Eliminar Producto (inicia flujo)
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:delete_product$") & filters.user(ADMIN_IDS))
async def shop_delete_product_cb(_, q: CallbackQuery):
    uid = q.from_user.id
    prods = get_products_by_admin(uid)
    if not prods:
        await q.answer()
        return await q.message.reply("🚫 No tienes productos para eliminar.")

    buttons = [
        [InlineKeyboardButton(p["name"], callback_data=f"shop:delete_product_select:{p['id']}")]
        for p in prods
    ]
    await q.message.reply("🗑️ Elige el producto a eliminar:", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


@bot.on_callback_query(filters.regex(r"^shop:delete_product_select:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_delete_product_select_cb(_, q: CallbackQuery):
    prod_id = int(q.data.split(":")[2])
    buttons = [
        [InlineKeyboardButton("✅ Sí", callback_data=f"shop:delete_product_final:{prod_id}")],
        [InlineKeyboardButton("❌ No", callback_data="shop:cancel")]
    ]
    await q.message.reply("⚠️ ¿Seguro que quieres eliminar este producto?", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


# ----------------------------
# 7) Eliminar Clasificación (inicia flujo)
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:delete_category$") & filters.user(ADMIN_IDS))
async def shop_delete_category_cb(_, q: CallbackQuery):
    cats = get_classifications()
    if not cats:
        await q.answer()
        return await q.message.reply("🚫 No hay categorías para eliminar.")

    buttons = [
        [InlineKeyboardButton(c["name"], callback_data=f"shop:delete_category_select:{c['id']}")]
        for c in cats
    ]
    await q.message.reply("🗑️ Elige la clasificación a eliminar:", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


@bot.on_callback_query(filters.regex(r"^shop:delete_category_select:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_delete_category_select_cb(_, q: CallbackQuery):
    cat_id = int(q.data.split(":")[2])
    buttons = [
        [InlineKeyboardButton("✅ Sí", callback_data=f"shop:delete_category_final:{cat_id}")],
        [InlineKeyboardButton("❌ No", callback_data="shop:cancel")]
    ]
    await q.message.reply("⚠️ ¿Seguro que quieres eliminar esta clasificación?", reply_markup=InlineKeyboardMarkup(buttons))
    await q.answer()


# ----------------------------
# 8) Callbacks de confirmación y flujo común
# ----------------------------
@bot.on_callback_query(filters.regex(r"^shop:delete_product_final:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_delete_product_final_cb(_, q: CallbackQuery):
    prod_id = int(q.data.split(":")[2])
    delete_product(prod_id)
    await q.message.reply("✅ Producto eliminado con éxito.")
    await q.answer()

@bot.on_callback_query(filters.regex(r"^shop:delete_category_final:(\d+)$") & filters.user(ADMIN_IDS))
async def shop_delete_category_final_cb(_, q: CallbackQuery):
    cat_id = int(q.data.split(":")[2])
    delete_category(cat_id)
    await q.message.reply("✅ Clasificación eliminada con éxito.")
    await q.answer()

@bot.on_callback_query(filters.regex(r"^shop:cancel$") & filters.user(ADMIN_IDS))
async def shop_cancel_cb(_, q: CallbackQuery):
    user_shop_states.pop(q.from_user.id, None)
    await q.message.reply("❎ Operación cancelada.")
    await q.answer()


# ----------------------------
# 9) Handler de texto y foto para flujos admin
# ----------------------------
@bot.on_message(filters.private & filters.create(
    lambda _, __, m: m.from_user.id in user_shop_states
))
async def shop_admin_flow(_, m: Message):
    uid = m.from_user.id
    state = user_shop_states[uid]
    text = (m.text or "").strip()

    # — Añadir Clasificación —
    if state["action"] == "add_category":
        save_category(text)
        await m.reply(f"✅ Clasificación «{text}» creada correctamente.")
        user_shop_states.pop(uid)
        return

    # — Añadir Producto —
    if state["action"] == "add_product":
        # 1) nombre → imagen
        if state["step"] == "name":
            state["name"] = text
            state["step"] = "image"
            return await m.reply("📷 Envía la *imagen del producto* o escribe `no` para omitir:")

        # 2) imagen opcional
        if state["step"] == "image":
            if m.photo:
                state["image_file_id"] = m.photo.file_id
            elif text.lower() == "no":
                state["image_file_id"] = None
            else:
                return await m.reply("🚫 Envía una foto o escribe `no` para omitir.")
            state["step"] = "desc"
            return await m.reply("📝 Ahora envía la *descripción* del producto:")

        # 3) descripción
        if state["step"] == "desc":
            state["desc"] = text
            state["step"] = "price"
            return await m.reply("💲 ¿Cuál es el *precio*?")

        # 4) precio
        if state["step"] == "price":
            state["price"] = text
            cats = get_classifications()
            buttons = [
                [InlineKeyboardButton(c["name"], callback_data=f"shop:select_cat:{c['id']}")]
                for c in cats
            ]
            buttons.append([InlineKeyboardButton("➕ Nueva Clasificación", callback_data="shop:add_category")])
            state["step"] = "category"
            return await m.reply("📂 Elige la clasificación:", reply_markup=InlineKeyboardMarkup(buttons))

        # 5) categoría → mensaje personalizado
        if state["step"] == "custom_msg":
            state["custom_msg"] = text if text.lower() != "no" else None
            save_product({
                "name": state["name"],
                "desc": state["desc"],
                "price": state["price"],
                "category_id": state["category_id"],
                "admin_id": uid,
                "admin_username": m.from_user.username or "",
                "custom_msg": state["custom_msg"],
                "image_file_id": state.get("image_file_id")
            })
            await m.reply("✅ Producto publicado con éxito.")
            user_shop_states.pop(uid)
            return

    # — Editar Producto —
    if state["action"] == "edit_product" and state.get("step") == "edit_value":
        field = state["field"]
        prod_id = state["prod_id"]

        # imagen
        if field == "image_file_id":
            if m.photo:
                val = m.photo[-1].file_id
            elif text.lower() == "no":
                val = None
            else:
                return await m.reply("🚫 Envía una foto o escribe `no` para omitir.")
        else:
            val = text

        update_product_field(prod_id, field, val)
        await m.reply(f"✅ Campo {field} actualizado correctamente.")
        user_shop_states.pop(uid)
        return

 

@bot.on_message(filters.command("queue") & filters.private & user_allowed_filter)
async def queue_command(_, m: Message):
    uid = m.from_user.id
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT queue_id, title, status, added_at FROM download_queue WHERE user_id=? AND status IN ('pending', 'processing') ORDER BY added_at ASC", (uid,))
    queue_items = cur.fetchall()
    conn.close()

    text = get_text(uid, "user_queue_title") + "\n\n"
    if not queue_items:
        text += get_text(uid, "user_queue_empty")
    else:
        for i, item in enumerate(queue_items):
            queue_id, title, status, added_at = item
            added_time = datetime.fromisoformat(added_at).strftime("%H:%M %d/%m")
            text += f"{i+1}. `{title}` - {status.capitalize()} (ID: {queue_id}) - {added_time}\n"
    
    await m.reply(text)

@bot.on_message(filters.command("getdb") & filters.private & filters.user(ADMIN_IDS))
async def get_db_command(_, m: Message):
    uid = m.from_user.id
    try:
        # Create a copy of the database to avoid locking issues
        db_copy_path = f"{DB_PATH}.copy"
        shutil.copyfile(DB_PATH, db_copy_path)
        
        await m.reply_document(db_copy_path, caption=get_text(uid, "db_file_caption"))
        os.remove(db_copy_path)
    except Exception as e:
        logging.error(f"Error sending database: {e}", exc_info=True)
        await m.reply(get_text(uid, "error_sending_db", error=e))


@bot.on_message(filters.command("admin") & filters.private & filters.user(ADMIN_IDS))
async def admin_panel(_, m: Message):
    uid = m.from_user.id
    # Cancel any active download or user detail update tasks for this admin
    for task in list(active_downloads_update_tasks.values()):
        task.cancel()
    active_downloads_update_tasks.clear()
    for task in list(user_detail_update_tasks.values()):
        task.cancel()
    user_detail_update_tasks.clear()

    await m.reply(f"{get_text(uid, 'admin_panel_title')}\n{separator()}", reply_markup=build_admin_menu(uid))

@bot.on_callback_query(filters.regex(r"^admin:dl_page:") & filters.user(ADMIN_IDS))
async def admin_dl_page(_, q: CallbackQuery):
    uid = q.from_user.id
    page = int(q.data.split(":")[2])
    ids = list(active_downloads.keys())
    per_pg = 5 # Increased to 5 downloads per page for more visibility
    start, end = page * per_pg, (page + 1) * per_pg
    chunk = ids[start:end]
    total_pages = max(1, (len(ids) + per_pg - 1) // per_pg)

    # Cancel any active download detail update tasks
    for task in list(active_downloads_update_tasks.values()):
        task.cancel()
    active_downloads_update_tasks.clear()

    text = get_text(uid, "admin_active_downloads_title", current=page + 1, total=total_pages) + "\n\n"
    if not chunk:
        text += get_text(uid, "admin_no_active_downloads")
    else:
        for idx, dl_id in enumerate(chunk, start + 1):
            dl = active_downloads[dl_id]
            s = dl["handle"].status()
            prog = s.progress * 100
            speed = format_bytes(s.download_rate)
            status_text = get_text(uid, "downloading_status") if s.state == lt.torrent_status.downloading else \
                          get_text(uid, "download_paused_status") if dl["paused"] else \
                          get_text(uid, "seeding_status") if s.state == lt.torrent_status.seeding else \
                          get_text(uid, "checking_files_status") if s.state == lt.torrent_status.checking_files else \
                          get_text(uid, "metadata_status") # Assuming metadata is an initial state

            text += (
                f"**{idx}. {dl['title']}**\n"
                f"  👤 @{dl['username']}\n"
                f"  📊 {status_text} ({prog:.1f}%)\n"
                f"  📈 {speed}/s\n"
                f"  📦 {format_bytes(dl['total_size_bytes'])}\n\n"
            )

    view_row = []
    for i, dl_id in enumerate(chunk):
        view_row.append(InlineKeyboardButton(get_text(uid, "admin_view_button", num=start + i + 1), callback_data=f"admin:dl_detail:{dl_id}:{page}"))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⏪ " + get_text(uid, "first_page_button"), callback_data=f"admin:dl_page:0"))
        nav.append(InlineKeyboardButton("⬅️ " + get_text(uid, "prev_page_button"), callback_data=f"admin:dl_page:{page - 1}"))
    if end < len(ids):
        nav.append(InlineKeyboardButton(get_text(uid, "next_page_button") + " ➡️", callback_data=f"admin:dl_page:{page + 1}"))
        nav.append(InlineKeyboardButton(get_text(uid, "last_page_button") + " ⏩", callback_data=f"admin:dl_page:{total_pages - 1}"))

    back_button = [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:back_main")]

    markup_rows = []
    if view_row:
        markup_rows.append(view_row)
    if nav:
        markup_rows.append(nav)
    markup_rows.append(back_button)

    markup = InlineKeyboardMarkup(markup_rows)
    
    # Only edit if the text or markup has changed
    if q.message.text != text or q.message.reply_markup != markup:
        await q.message.edit(text=text, reply_markup=markup)
    await q.answer()

@bot.on_callback_query(filters.regex(r"^admin:dl_detail:") & filters.user(ADMIN_IDS))
async def admin_dl_detail(_, q: CallbackQuery):
    uid = q.from_user.id
    _, _, dl_id, page = q.data.split(":")
    dl = active_downloads.get(dl_id)
    if not dl:
        await q.answer(get_text(uid, "admin_download_not_found"), show_alert=True)
        # If the download is not found, redirect to the active downloads page
        return await admin_dl_page(_, q) 

    # Cancel any previous download detail update task
    if dl_id in active_downloads_update_tasks:
        active_downloads_update_tasks[dl_id].cancel()
        active_downloads_update_tasks.pop(dl_id, None)

    text = _build_dl_detail_text(uid, dl)
    buttons = _build_dl_detail_buttons(uid, dl_id, dl, page)
    
    # Only edit if the text or markup has changed
    if q.message.text != text or q.message.reply_markup != buttons:
        await q.message.edit(text, reply_markup=buttons)
    await q.answer()

    # Start the automatic update task for this download
    active_downloads_update_tasks[dl_id] = asyncio.create_task(
        update_dl_detail_periodically(q.message, uid, dl_id, page)
    )

@bot.on_callback_query(filters.regex(r"^admin:action:") & filters.user(ADMIN_IDS))
async def admin_action(_, q: CallbackQuery):
    uid = q.from_user.id
    # Ejemplo de cómo obtener 'username'
    username = q.from_user.username

    action_parts = q.data.split(":")
    action = action_parts[2]
    target = action_parts[3]
    page = int(action_parts[4]) if len(action_parts) > 4 else 0 # Return page

    # Cancel any active download or user detail update tasks
    for task in list(active_downloads_update_tasks.values()):
        task.cancel()
    active_downloads_update_tasks.clear()
    for task in list(user_detail_update_tasks.values()):
        task.cancel()
    user_detail_update_tasks.clear()

    if action == "toggle_pause_resume":
        dl = active_downloads.get(target)
        if dl:
            dl["paused"] = not dl["paused"]
            if not dl["paused"]: # If resumed, update start time for ETA
                dl["start"] = time.time()
            await q.answer(get_text(uid, "download_resumed") if not dl["paused"] else get_text(uid, "download_paused"))
            # Re-show details to update the button and restart the update task
            await admin_dl_detail(_, q) 
        else:
            await q.answer(get_text(uid, "admin_download_not_found"), show_alert=True)
        return # Do not redirect to dl_page, stay on detail

    elif action == "confirm_cancel":
        dl = active_downloads.get(target)
        if not dl:
            await q.answer(get_text(uid, "admin_download_not_found"), show_alert=True)
            return await admin_dl_page(_, q)
        
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "confirm_yes"), callback_data=f"admin:action:cancel:{target}:{page}")],
            [InlineKeyboardButton(get_text(uid, "confirm_no"), callback_data=f"admin:dl_detail:{target}:{page}")]
        ])
        new_text = get_text(uid, "admin_confirm_cancel_download_message", title=dl['title'])
        new_markup = buttons
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        await q.answer()
        return

    elif action == "cancel":
        dl = active_downloads.pop(target, None)
        if dl:
            try:
                dl["handle"].session.remove_torrent(dl["handle"])
                await q.answer(get_text(uid, "admin_download_cancelled"))
            except Exception as e:
                logging.error(f"Error removing torrent {target}: {e}")
                await q.answer(get_text(uid, "admin_error_removing_torrent"), show_alert=True)
        else:
            await q.answer(get_text(uid, "admin_download_not_found"), show_alert=True)
        await admin_dl_page(_, q) # Return to downloads list
        return

    elif action == "confirm_block":
        target_uid = int(target)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT username FROM users WHERE user_id=?", (target_uid,))
        user_data = cur.fetchone()
        conn.close()
        username = user_data[0] if user_data else f"user{target_uid}"

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "confirm_yes"), callback_data=f"admin:action:block:{target_uid}:{page}")],
            [InlineKeyboardButton(get_text(uid, "confirm_no"), callback_data=f"admin:user_detail:{target_uid}:{page}" if action_parts[1] == "user_detail" else f"admin:dl_detail:{target}:{page}")]
        ])
        new_text = get_text(target_uid, "admin_block_user_confirm_message", username=username)
        new_markup = buttons
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        await q.answer()
        return

    elif action == "block":
        target_uid = int(target)
        if block_user_db(target_uid):
            await q.answer(get_text(uid, "admin_user_blocked", username=username))
        else:
            await q.answer(get_text(uid, "admin_error_blocking_user"), show_alert=True)
        
        # Determine which menu to return to
        if q.data.startswith("admin:action:confirm_block") and len(action_parts) > 5 and action_parts[1] == "dl_detail": # If coming from block confirmation from dl_detail
            await admin_dl_page(_, q) # Return to downloads list
        else: # If coming from block confirmation from user_detail or all_users
            await admin_all_users_page(_, q) # Return to all users list
        return

    elif action == "confirm_unblock":
        target_uid = int(target)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT username FROM users WHERE user_id=?", (target_uid,))
        user_data = cur.fetchone()
        conn.close()
        username = user_data[0] if user_data else f"user{target_uid}"

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "confirm_yes"), callback_data=f"admin:action:unblock:{target_uid}:{page}")],
            [InlineKeyboardButton(get_text(uid, "confirm_no"), callback_data=f"admin:user_detail:{target_uid}:{page}")]
        ])
        new_text = get_text(uid, "admin_unblock_user_confirm_message", username=username)
        new_markup = buttons
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
        await q.answer()
        return

    elif action == "unblock":
        target_uid = int(target)
        if unblock_user_db(target_uid):
            await q.answer(get_text(uid, "admin_user_unblocked", usernsme=username))
        else:
            await q.answer(get_text(uid, "admin_error_unblocking_user"), show_alert=True)
        
        # Always return to the list of all users or blocked users
        if q.data.startswith("admin:action:confirm_unblock"): # If coming from unblock confirmation from user_detail
            await admin_all_users_page(_, q) # Return to all users list
        else: # If coming from the blocked users list
            await admin_users(_, q)
        return

    elif action == "preview":
        dl = active_downloads.get(target)
        if not dl:
            return await q.answer(get_text(uid, "admin_download_not_found"), show_alert=True)
        
        try:
            files = dl["handle"].get_torrent_info().files()
            preview = get_text(uid, "admin_files_list") + "\n" + "\n".join(
                f"• {f.path} — {format_bytes(f.size)}" for f in files
            )
            new_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data=f"admin:dl_detail:{target}:{page}")]
            ])
            if q.message.text != preview or q.message.reply_markup != new_markup:
                await q.message.edit(preview, reply_markup=new_markup)
            return await q.answer()
        except Exception as e:
            logging.error(f"Error getting files for {target}: {e}")
            await q.answer(get_text(uid, "admin_error_getting_files"), show_alert=True)
            return
    
    # If the action is not detail or confirmation, return to the active downloads page by default
    # This may need to be adjusted if there are more actions that do not redirect to dl_page
    await admin_dl_page(_, q)


@bot.on_callback_query(filters.regex(r"^admin:stats$") & filters.user(ADMIN_IDS))
async def admin_stats(_, q: CallbackQuery):
    uid = q.from_user.id
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM users");        total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM user_torrents");total_torrents = cur.fetchone()[0]
    cur.execute("SELECT SUM(size_bytes) FROM user_torrents");total_gb_downloaded_bytes = cur.fetchone()[0] or 0
    conn.close()

    total_gb_downloaded = format_bytes(total_gb_downloaded_bytes)

    total_download_rate = 0
    total_upload_rate = 0
    for dl_id in active_downloads:
        dl = active_downloads[dl_id]
        s = dl["handle"].status()
        total_download_rate += s.download_rate
        total_upload_rate += s.upload_rate
    
    text = (
        f"{get_text(uid, 'admin_global_stats_title')}\n\n"
        f"{get_text(uid, 'admin_total_users', count=total_users)}\n"
        f"{get_text(uid, 'admin_total_torrents', count=total_torrents)}\n"
        f"{get_text(uid, 'admin_total_downloaded_gb', size=total_gb_downloaded)}\n"
        f"{get_text(uid, 'admin_current_bandwidth')}\n"
        f"  {get_text(uid, 'admin_download_rate', speed=format_bytes(total_download_rate))}/s\n"
        f"  {get_text(uid, 'admin_upload_rate', speed=format_bytes(total_upload_rate))}/s"
    )
    buttons = [[InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:back_main")]]
    new_markup = InlineKeyboardMarkup(buttons)
    if q.message.text != text or q.message.reply_markup != new_markup:
        await q.message.edit(text, reply_markup=new_markup)
    await q.answer()

# MODIFIED: admin_users now returns to the Management menu
@bot.on_callback_query(filters.regex(r"^admin:users$") & filters.user(ADMIN_IDS))
async def admin_users(_, q: CallbackQuery):
    uid = q.from_user.id
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Explicitly qualify user_id to avoid ambiguity
    cur.execute("SELECT T1.user_id, T2.username FROM blocked_users AS T1 LEFT JOIN users AS T2 ON T1.user_id = T2.user_id")
    rows = cur.fetchall()
    conn.close()
    
    text = get_text(uid, "admin_blocked_users_title") + "\n\n"
    if not rows:
        text += get_text(uid, "admin_no_blocked_users")
        buttons = [[InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:management")]] # Return to Management
    else:
        user_buttons = []
        for user_id, username in rows:
            display_name = username if username else f"user{user_id}"
            text += f"• {display_name} (ID: {user_id})\n"
            user_buttons.append(InlineKeyboardButton(get_text(uid, "admin_unblock_user_button", username=display_name), callback_data=f"admin:action:confirm_unblock:{user_id}:0")) # Confirmation
        buttons = [user_buttons, [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:management")]] # Return to Management

    new_markup = InlineKeyboardMarkup(buttons)
    if q.message.text != text or q.message.reply_markup != new_markup:
        await q.message.edit(text, reply_markup=new_markup)
    await q.answer()

# MODIFIED: admin_maintenance_menu to include "Clear Temporary Files"
@bot.on_callback_query(filters.regex(r"^admin:maintenance$") & filters.user(ADMIN_IDS))
async def admin_maintenance_menu(_, q: CallbackQuery):
    uid = q.from_user.id
    buttons = [
        [InlineKeyboardButton(
            (get_text(uid, "admin_maintenance_deactivated") if MAINTENANCE_MODE else get_text(uid, "admin_maintenance_activated")) + " " + get_text(uid, "admin_toggle_maintenance_mode_text"),
            callback_data="admin:maint:toggle_pause"
        )],
        [InlineKeyboardButton(
            (get_text(uid, "admin_search_enabled") if SEARCH_DISABLED else get_text(uid, "admin_search_disabled")) + " " + get_text(uid, "admin_toggle_search_text"),
            callback_data="admin:maint:toggle_search"
        )],
        [InlineKeyboardButton(get_text(uid, "admin_clear_cache"), callback_data="admin:maint:clear_cache")],
        [InlineKeyboardButton(get_text(uid, "admin_temp_files"), callback_data="admin:maint:clear_temp_files")], # MOVED HERE
        [InlineKeyboardButton(get_text(uid, "admin_audit_downloads"), callback_data="admin:maint:audit_downloads")],
        [InlineKeyboardButton(get_text(uid, "admin_reload_libtorrent"), callback_data="admin:maint:reload_libtorrent")],
        [InlineKeyboardButton(get_text(uid, "admin_view_logs"), callback_data="admin:maint:view_logs")],
        [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:back_main")]
    ]
    new_markup = InlineKeyboardMarkup(buttons)
    new_text = f"{get_text(uid, 'admin_maintenance_title')}\n{separator()}"
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)
    await q.answer()

# MODIFIED: admin_maintenance_actions to include "clear_temp_files"
@bot.on_callback_query(filters.regex(r"^admin:maint:") & filters.user(ADMIN_IDS))
async def admin_maintenance_actions(_, q: CallbackQuery):
    global MAINTENANCE_MODE, SEARCH_DISABLED
    uid = q.from_user.id
    action = q.data.split(":")[2]
    if action == "toggle_pause":
        MAINTENANCE_MODE = not MAINTENANCE_MODE
        await q.answer(get_text(uid, "admin_maintenance_activated_status" if MAINTENANCE_MODE else "admin_maintenance_deactivated_status"), show_alert=True)
        return await admin_maintenance_menu(_, q)
    if action == "toggle_search":
        SEARCH_DISABLED = not SEARCH_DISABLED
        await q.answer(get_text(uid, "admin_search_disabled_status" if SEARCH_DISABLED else "admin_search_enabled_status"), show_alert=True)
        return await admin_maintenance_menu(_, q)
    if action == "clear_cache":
        user_search_states.clear()
        await q.answer(get_text(uid, "admin_cache_cleared"), show_alert=True)
        return await admin_maintenance_menu(_, q)
    if action == "clear_temp_files": # NEW ACTION
        temp_dir = tempfile.gettempdir()
        deleted_count = 0
        deleted_size = 0
        for filename in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, filename)
            try:
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    os.remove(file_path)
                    deleted_count += 1
                    deleted_size += file_size
            except Exception as e:
                logging.warning(f"Could not delete temporary file {file_path}: {e}")
        await q.answer(get_text(uid, "admin_temp_files_cleared", count=deleted_count, size=format_bytes(deleted_size)), show_alert=True)
        return await admin_maintenance_menu(_, q)
    if action == "audit_downloads":
        lines = [
            f"{dl['title']}: {dl['handle'].status().progress*100:.1f}%"
            for dl in active_downloads.values()
        ]
        text = get_text(uid, "admin_audit_downloads_list") + "\n\n" + ("\n".join(lines) if lines else get_text(uid, "admin_no_active_audit"))
        new_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:maintenance")]
        ])
        if q.message.text != text or q.message.reply_markup != new_markup:
            await q.message.edit(text, reply_markup=new_markup)
        return await q.answer()
    if action == "reload_libtorrent":
        await q.answer(get_text(uid, "admin_libtorrent_reload_requested"), show_alert=True)
        return await admin_maintenance_menu(_, q)
    if action == "view_logs":
        logs = tail_logs("bot.log", 8)
        text = f"{get_text(uid, 'admin_recent_logs')}\n\n{logs}"
        new_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:maintenance")]
        ])
        if q.message.text != text or q.message.reply_markup != new_markup:
            await q.message.edit(text, reply_markup=new_markup)
        return await q.answer()
    if action == "back_main":
        return await admin_panel(_, q.message)

@bot.on_callback_query(filters.regex(r"^admin:queue$") & filters.user(ADMIN_IDS))
async def admin_queue_menu(_, q: CallbackQuery):
    uid = q.from_user.id
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT queue_id, title, status, added_at FROM download_queue WHERE status IN ('pending', 'processing') ORDER BY added_at ASC")
    queue_items = cur.fetchall()
    conn.close()

    text = get_text(uid, "admin_queue_title") + "\n\n"
    if not queue_items:
        text += get_text(uid, "admin_queue_empty")
    else:
        for i, item in enumerate(queue_items):
            queue_id, title, status, added_at = item
            added_time = datetime.fromisoformat(added_at).strftime("%H:%M %d/%m")
            text += f"{i+1}. `{title}` - {status.capitalize()} (ID: {queue_id}) - {added_time}\n"
    
    text += f"\n{get_text(uid, 'admin_queue_processing_count', count=len(processing_downloads), max_simultaneous=MAX_SIMULTANEOUS_DOWNLOADS)}"

    buttons = [
        [InlineKeyboardButton(get_text(uid, "admin_refresh_queue"), callback_data="admin:queue")],
        [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:back_main")]
    ]
    new_markup = InlineKeyboardMarkup(buttons)
    if q.message.text != text or q.message.reply_markup != new_markup:
        await q.message.edit(text, reply_markup=new_markup)
    await q.answer()

# NEW: Callback for the Management menu
@bot.on_callback_query(filters.regex(r"^admin:management$") & filters.user(ADMIN_IDS))
async def admin_management(_, q: CallbackQuery):
    uid = q.from_user.id
    new_text = f"{get_text(uid, 'admin_management_title')}\n{separator()}"
    new_markup = build_management_menu(uid)
    if q.message.text != new_text or q.message.reply_markup != new_markup:
        await q.message.edit(new_text, reply_markup=new_markup)
    await q.answer()

# NEW: Pagination for all users
@bot.on_callback_query(filters.regex(r"^admin:all_users_page:") & filters.user(ADMIN_IDS))
async def admin_all_users_page(_, q: CallbackQuery):
    uid = q.from_user.id
    page = int(q.data.split(":")[2])
    per_pg = 5  # Users per page

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, joined_at, upload_mode, language_code FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?", 
               (per_pg, page * per_pg))
    users_data = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM users")
    total_users_count = cur.fetchone()[0]
    conn.close()

    total_pages = max(1, (total_users_count + per_pg - 1) // per_pg)
    end = (page + 1) * per_pg  # Define end here

    text = get_text(uid, "admin_all_users_title", current=page + 1, total=total_pages) + "\n\n"
    if not users_data:
        text += get_text(uid, "admin_no_users_found")
    else:
        for idx, user_row in enumerate(users_data, page * per_pg + 1):
            user_id, username, joined_at, upload_mode, language_code = user_row
            
            # Get download count for each user
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM user_torrents WHERE user_id=?", (user_id,))
            total_downloads = cur.fetchone()[0]
            conn.close()

            display_name = username if username else f"user{user_id}"
            text += (
                f"**{idx}. @{display_name} (ID: {user_id})**\n"
                f"  • {get_text(uid, 'admin_user_downloads_count', count=total_downloads)}\n"
                f"  • {get_text(uid, 'admin_user_upload_mode', mode=upload_mode.capitalize())}\n\n"
            )

    view_row = []
    for i, user_row in enumerate(users_data):
        view_row.append(InlineKeyboardButton(
            get_text(uid, "admin_view_button", num=page * per_pg + i + 1), 
            callback_data=f"admin:user_detail:{user_row[0]}:{page}"
        ))

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⏪ " + get_text(uid, "first_page_button"), 
                                      callback_data=f"admin:all_users_page:0"))
        nav.append(InlineKeyboardButton("⬅️ " + get_text(uid, "prev_page_button"), 
                                      callback_data=f"admin:all_users_page:{page - 1}"))
    if end < len(users_data) + (page * per_pg):  # Fixed end comparison
        nav.append(InlineKeyboardButton(get_text(uid, "next_page_button") + " ➡️", 
                                      callback_data=f"admin:all_users_page:{page + 1}"))
        nav.append(InlineKeyboardButton(get_text(uid, "last_page_button") + " ⏩", 
                                      callback_data=f"admin:all_users_page:{total_pages - 1}"))

    back_button = [InlineKeyboardButton(get_text(uid, "settings_back"), callback_data="admin:management")]

    markup_rows = []
    if view_row:
        markup_rows.append(view_row)
    if nav:
        markup_rows.append(nav)
    markup_rows.append(back_button)

    markup = InlineKeyboardMarkup(markup_rows)
    if q.message.text != text or q.message.reply_markup != markup:
        await q.message.edit(text=text, reply_markup=markup)
    await q.answer()


# NEW: User detail
@bot.on_callback_query(filters.regex(r"^admin:user_detail:") & filters.user(ADMIN_IDS))
async def admin_user_detail(_, q: CallbackQuery):
    uid = q.from_user.id
    _, _, target_user_id_str, page_str = q.data.split(":")
    target_user_id = int(target_user_id_str)
    current_page = int(page_str)

    # Cancel any previous user detail update task
    if target_user_id in user_detail_update_tasks:
        user_detail_update_tasks[target_user_id].cancel()
        user_detail_update_tasks.pop(target_user_id, None)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id, username, joined_at, upload_mode, language_code FROM users WHERE user_id=?", (target_user_id,))
    user_data_row = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM user_torrents WHERE user_id=?", (target_user_id,))
    total_downloads = cur.fetchone()[0]
    conn.close()

    if not user_data_row:
        await q.answer(get_text(uid, "admin_user_not_found"), show_alert=True)
        return await admin_all_users_page(_, q)

    user_info = {
        'user_id': user_data_row[0],
        'username': user_data_row[1],
        'joined_at': user_data_row[2],
        'upload_mode': user_data_row[3],
        'language_code': user_data_row[4],
        'total_downloads': total_downloads
    }

    text = _build_user_detail_text(uid, user_info)
    buttons = _build_user_detail_buttons(uid, target_user_id, current_page)

    new_markup = buttons
    if q.message.text != text or q.message.reply_markup != new_markup:
        await q.message.edit(text, reply_markup=new_markup)
    await q.answer()

    # Start the automatic update task for this user
    user_detail_update_tasks[target_user_id] = asyncio.create_task(
        update_user_detail_periodically(q.message, uid, target_user_id, current_page)
    )


# SEARCH COMMANDS & CALLBACKS
@bot.on_message(filters.command("search") & filters.private & search_allowed_filter)
async def cmd_search(_, m: Message):
    uid = m.from_user.id
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return await m.reply(get_text(uid, "search_usage"))
    term = parts[1].strip()
    buttons = [
        [InlineKeyboardButton("🔎 TPB", callback_data=f"search:src:tpb:{term}")],
        [InlineKeyboardButton("🧲 MT",  callback_data=f"search:src:mt:{term}")],
        [InlineKeyboardButton("📺 Nyaa",callback_data=f"search:src:nyaa:{term}")],
        [InlineKeyboardButton("🎬 YTS",callback_data=f"search:src:yts:{term}")],
        [InlineKeyboardButton("🎮 GT", callback_data=f"search:src:gt:{term}")],
        [InlineKeyboardButton("🌐 All",callback_data=f"search:src:all:{term}")]
    ]
    await m.reply(get_text(uid, "choose_source"), reply_markup=InlineKeyboardMarkup(buttons))

@bot.on_callback_query(filters.regex(r"^search:src:") & search_allowed_filter)
async def search_source_handler(_, q: CallbackQuery):
    _, _, src, term = q.data.split(":", 3)
    uid = q.from_user.id
    results = []
    try:
        new_text = get_text(uid, "searching_message", source=src.upper(), term=term)
        if q.message.text != new_text:
            await q.message.edit(new_text)
        if src in ("tpb","all"):  results.extend(await search_piratebay(term))
        if src in ("mt","all"):   results.extend(await search_mejortorrent(term))
        if src in ("nyaa","all"): results.extend(await search_nyaa(term))
        if src in ("yts","all"):  results.extend(await search_yts(term))
        if src in ("gt","all"):   results.extend(await search_gamestorrents(term))
    except Exception as e:
        logging.error(f"Error in search for {src} with term '{term}': {e}", exc_info=True)
        new_text = get_text(uid, "error_searching", src=src, error=e)
        if q.message.text != new_text:
            await q.message.edit(new_text)
        return await q.answer()
    
    if not results:
        new_text = get_text(uid, "no_results")
        if q.message.text != new_text:
            await q.message.edit(new_text)
        return await q.answer()
    
    user_search_states[uid] = {
        "results":  results,
        "page":     0,
        "per_page": 5,
        "message_id": q.message.id
    }
    await show_search_list(q.message, uid)
    await q.answer()

async def show_search_list(message: Message, user_id: int):
    state = user_search_states[user_id]
    page, per_page = state["page"], state["per_page"]
    start, end = page*per_page, (page+1)*per_page
    chunk = state["results"][start:end]
    text = get_text(user_id, "search_results_title") + "\n\n"
    for i, item in enumerate(chunk, start+1):
        text += f"{i}. {item['title']}\n"
        if item.get("magnets"):
            text += f"   🎞️ {len(item['magnets'])} {get_text(user_id, 'versions_text')}\n\n"
        else:
            text += f"   📦 {item.get('size','N/A')}   🎭 {item.get('category','N/A')}\n\n"
    total_pages = (len(state["results"])+per_page-1)//per_page
    text += get_text(user_id, "page_indicator", current=page+1, total=total_pages)
    
    dl_row = []
    for i in range(start+1, min(end,len(state["results"]))+1):
        dl_row.append(InlineKeyboardButton(get_text(user_id, "download_button", num=i), callback_data=f"search:dl:{i-1}"))

    nav = []
    if page>0:    nav.append(InlineKeyboardButton(get_text(user_id, "prev_page_button"), callback_data="search:page:prev"))
    if end<len(state["results"]): nav.append(InlineKeyboardButton(get_text(user_id, "next_page_button"), callback_data="search:page:next"))
    nav.append(InlineKeyboardButton(get_text(user_id, "cancel_button"), callback_data="search:page:cancel"))
    
    markup_rows = []
    if dl_row: markup_rows.append(dl_row)
    markup_rows.append(nav)

    new_markup = InlineKeyboardMarkup(markup_rows)
    if message.text != text or message.reply_markup != new_markup:
        await bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=state["message_id"],
            text=text,
            reply_markup=new_markup
        )

@bot.on_callback_query(filters.regex(r"^search:page:") & search_allowed_filter)
async def search_page_handler(_, q: CallbackQuery):
    uid    = q.from_user.id
    action = q.data.split(":")[2]
    state  = user_search_states.get(uid)
    if not state:
        await q.answer(get_text(uid, "session_expired"), show_alert=True)
        await q.message.delete()
        return
    
    if action == "prev":
        state["page"] = max(0, state["page"]-1)
        await show_search_list(q.message, uid)
    elif action == "next":
        if (state["page"]+1)*state["per_page"] < len(state["results"]):
            state["page"] += 1
            await show_search_list(q.message, uid)
    elif action == "cancel":
        user_search_states.pop(uid, None)
        await q.message.delete()
    await q.answer()

@bot.on_callback_query(filters.regex(r"^search:dl:") & search_allowed_filter)
async def search_choose_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    idx = int(q.data.split(":")[2])
    
    state = user_search_states.get(uid)
    if not state or idx >= len(state["results"]):
        await q.answer(get_text(uid, "invalid_result"), show_alert=True)
        await q.message.delete()
        return

    item = state["results"][idx]
    
    if item.get("magnets"):
        buttons = [
            [InlineKeyboardButton(f"{m['quality']} — {m['size']}",
                callback_data=f"search:start_yts:{idx}:{midx}")]
            for midx, m in enumerate(item["magnets"])
        ] + [[InlineKeyboardButton(get_text(uid, "back_button"), callback_data="search:page:same")]]
        new_markup = InlineKeyboardMarkup(buttons)
        new_text = get_text(uid, "yts_choose_quality", title=item['title'])
        if q.message.text != new_text or q.message.reply_markup != new_markup:
            await q.message.edit(new_text, reply_markup=new_markup)
    else:
        text = (
            f"🏷️ {item['title']}\n"
            f"📦 {item.get('size','N/A')}   🎭 {item.get('category','N/A')}\n"
            f"🌐 {get_text(uid, 'source_label')}: {item.get('source','N/A')}"
        )
        buttons = [
            [InlineKeyboardButton(get_text(uid, "download_button_generic"), callback_data=f"search:start:{idx}")],
            [InlineKeyboardButton(get_text(uid, "torrent_button"), callback_data=f"search:get:{idx}")],
            [InlineKeyboardButton(get_text(uid, "back_button"), callback_data="search:page:same")]
        ]
        new_markup = InlineKeyboardMarkup(buttons)
        if q.message.text != text or q.message.reply_markup != new_markup:
            await q.message.edit(text, reply_markup=new_markup)
    await q.answer()

@bot.on_callback_query(filters.regex(r"^search:start_yts:") & search_allowed_filter)
async def search_start_yts_handler(_, q: CallbackQuery):
    _, _, idx_str, midx_str = q.data.split(":")
    idx, midx = int(idx_str), int(midx_str)
    uid, chat_id = q.from_user.id, q.message.chat.id
    
    state = user_search_states.get(uid)
    if not state or idx >= len(state["results"]) or midx >= len(state["results"][idx].get("magnets", [])):
        await q.answer(get_text(uid, "invalid_result"), show_alert=True)
        await q.message.delete()
        return

    item = state["results"][idx]
    magnet = item["magnets"][midx]["magnet_link"]
    calidad = item["magnets"][midx]["quality"]
    
    await add_to_download_queue(uid, chat_id, magnet, f"{item['title']} ({calidad})")
    user_search_states.pop(uid, None)
    await q.message.delete()
    await q.answer()

@bot.on_callback_query(filters.regex(r"^search:start:") & search_allowed_filter)
async def search_start_download_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    idx = int(q.data.split(":")[2])
    
    state = user_search_states.get(uid)
    if not state or idx >= len(state["results"]):
        await q.answer(get_text(uid, "invalid_result"), show_alert=True)
        await q.message.delete()
        return

    item = state["results"][idx]
    magnet = item.get("magnet_link")
    torrent_path = item.get("torrent_path")

    if not magnet and torrent_path:
        try:
            data = open(torrent_path, "rb").read()
            info = lt.torrent_info(lt.bdecode(data))
            magnet = lt.make_magnet_uri(info)
        except Exception as e:
            logging.error(f"Error converting .torrent to magnet for {item['title']}: {e}")
            magnet = None
        finally:
            if os.path.exists(torrent_path):
                try:
                    os.remove(torrent_path)
                except OSError as e:
                    logging.warning(f"Could not delete temporary file {torrent_path}: {e}")

    if not magnet:
        await q.answer(get_text(uid, "source_not_available"), show_alert=True)
        user_search_states.pop(uid, None)
        await q.message.delete()
        return
    
    await add_to_download_queue(uid, q.message.chat.id, magnet, item['title'])
    user_search_states.pop(uid, None)
    await q.message.delete()
    await q.answer()

@bot.on_callback_query(filters.regex(r"^search:get:") & search_allowed_filter)
async def search_get_torrent_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    idx = int(q.data.split(":")[2])
    
    state = user_search_states.get(uid)
    if not state or idx >= len(state["results"]):
        await q.answer(get_text(uid, "invalid_result"), show_alert=True)
        await q.message.delete()
        return

    item = state["results"][idx]
    chat_id = q.message.chat.id
    torrent_path = item.get("torrent_path")
    
    status_msg = None
    try:
        if not torrent_path:
            if not item.get("magnet_link"):
                await q.answer(get_text(uid, "no_magnet_for_torrent"), show_alert=True)
                return

            status_msg = await bot.send_message(chat_id, get_text(uid, "getting_metadata_for_torrent"))
            ses = lt.session()
            ses.listen_on(6881, 6891)
            handle = lt.add_magnet_uri(ses, item["magnet_link"], {"save_path": tempfile.gettempdir(), "storage_mode": lt.storage_mode_t.storage_mode_sparse})
            
            timeout_meta = 60
            start_meta = time.time()
            while not handle.has_metadata():
                if time.time() - start_meta > timeout_meta:
                    new_text = get_text(uid, "metadata_timeout_for_torrent")
                    if status_msg.text != new_text:
                        await status_msg.edit(new_text)
                    ses.remove_torrent(handle)
                    return await q.answer()
                await asyncio.sleep(1)
            
            ti = handle.get_torrent_info()
            crt = lt.create_torrent(ti)
            data = lt.bencode(crt.generate())
            name = ti.name().replace(" ", "_")
            torrent_path = os.path.join(tempfile.gettempdir(), f"{name}.torrent")
            with open(torrent_path, "wb") as f:
                f.write(data)
            ses.remove_torrent(handle)
            new_text = get_text(uid, "metadata_ready_sending")
            if status_msg.text != new_text:
                await status_msg.edit(new_text)
        
        await bot.send_document(chat_id, torrent_path, caption=f"📦 {item['title']}.torrent")
        await q.answer(get_text(uid, "here_is_your_torrent"), show_alert=True)

    except Exception as e:
        logging.error(f"Error getting/sending .torrent for {item['title']}: {e}", exc_info=True)
        if status_msg:
            new_text = get_text(uid, "error_getting_torrent", error=e)
            if status_msg.text != new_text:
                await status_msg.edit(new_text)
        else:
            await q.answer(get_text(uid, "error_getting_torrent", error=e), show_alert=True)
    finally:
        if torrent_path and os.path.exists(torrent_path):
            try:
                os.remove(torrent_path)
            except OSError as e:
                logging.warning(f"Could not delete temporary file {torrent_path}: {e}")
        user_search_states.pop(uid, None)
        await q.message.delete()

@bot.on_callback_query(filters.regex(r"^search:page:same$") & search_allowed_filter)
async def search_page_same_handler(_, q: CallbackQuery):
    uid = q.from_user.id
    if uid in user_search_states:
        await show_search_list(q.message, uid)
        await q.answer()
    else:
        await q.answer(get_text(uid, "session_expired"), show_alert=True)
        await q.message.delete()

# HANDLING .TORRENT AND MAGNET LINKS
@bot.on_message(filters.document & filters.private & user_allowed_filter)
async def handle_torrent_file(_, m: Message):
    uid = m.from_user.id
    if not m.document.file_name or not m.document.file_name.lower().endswith(".torrent"):
        return await m.reply(get_text(uid, "invalid_torrent_file"))
    
    status = await m.reply(get_text(uid, "processing_torrent_file"))
    path = None
    try:
        path = await m.download()
        data = open(path, "rb").read()
        info = lt.torrent_info(lt.bdecode(data))
        magnet = lt.make_magnet_uri(info)
        await add_to_download_queue(uid, m.chat.id, magnet, info.name())
        new_text = get_text(uid, "torrent_file_added_to_queue")
        if status.text != new_text:
            await status.edit(new_text)
    except Exception as e:
        logging.error(f"Error processing .torrent file: {e}", exc_info=True)
        new_text = get_text(uid, "error_processing_torrent", error=e)
        if status.text != new_text:
            await status.edit(new_text)
    finally:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError as e:
                logging.warning(f"Could not delete temporary file {path}: {e}")

@bot.on_message(filters.text & filters.private & user_allowed_filter)
async def handle_text(_, m: Message):
    uid, txt = m.from_user.id, m.text.strip()
    
    # Handle broadcast state
    if uid in broadcast_state and broadcast_state[uid] == "pending_message":
        await send_broadcast_confirmation(m.chat.id, txt, uid)
        broadcast_state.pop(uid, None) # Clear state after getting the message
        return
    
    # Handle Google Drive authorization code
    if uid in user_search_states and user_search_states[uid].get("awaiting_drive_code"):
        flow = user_search_states[uid].get("drive_flow")
        if flow:
            try:
                flow.fetch_token(code=txt)
                creds = flow.credentials
                save_google_drive_credentials(uid, creds)
                await m.reply(get_text(uid, "drive_auth_success"))
                user_search_states[uid].pop("awaiting_drive_code", None)
                user_search_states[uid].pop("drive_flow", None)
                await show_drive_menu(m, uid)
            except Exception as e:
                logging.error(f"Error exchanging Drive code for {uid}: {e}")
                await m.reply(get_text(uid, "drive_auth_error", error=e))
        else:
            await m.reply(get_text(uid, "drive_no_active_session"))
        return

    # Handle Google Drive folder creation
    if uid in user_search_states and user_search_states[uid].get("awaiting_folder_name"):
        folder_name = txt
        parent_folder_id = user_search_states[uid].get("parent_folder_id", "root")
        service = await get_authenticated_drive_service(uid, m)
        if service:
            try:
                folder_id, name = await create_drive_folder(service, folder_name, parent_folder_id)
                await m.reply(get_text(uid, "drive_folder_created", folder_name=name))
                user_search_states[uid].pop("awaiting_folder_name", None)
                user_search_states[uid].pop("parent_folder_id", None)
                await show_drive_menu(m, uid, current_folder_id=parent_folder_id)
            except HttpError as e:
                logging.error(f"Error creating folder in Drive for {uid}: {e}")
                await m.reply(get_text(uid, "drive_create_folder_error", error=e))
            except Exception as e:
                logging.error(f"Unexpected error creating folder in Drive for {uid}: {e}")
                await m.reply(get_text(uid, "drive_unexpected_error_generic", error=e))
        else:
            await m.reply(get_text(uid, "drive_connection_error"))
        return

    if txt.startswith("magnet:?xt=urn:btih:"):
        if not validate_magnet_link(txt):
            return await m.reply(get_text(uid, "invalid_magnet_link"))
        
        # Try to get the title from the magnet for the queue
        title = "Unknown Torrent"
        try:
            ses = lt.session()
            ses.listen_on(6881, 6891)
            params = {"save_path": tempfile.gettempdir(), "storage_mode": lt.storage_mode_t.storage_mode_sparse}
            handle = lt.add_magnet_uri(ses, txt, params)
            # Wait a bit for metadata to be obtained if possible
            for _ in range(5): # Try 5 times with 1 second wait
                if handle.has_metadata():
                    title = handle.get_torrent_info().name()
                    break
                await asyncio.sleep(1)
            ses.remove_torrent(handle)
        except Exception as e:
            logging.warning(f"Could not get title from magnet link {txt}: {e}")

        await add_to_download_queue(uid, m.chat.id, txt, title)
        await m.reply(get_text(uid, "magnet_added_to_queue"))
    # else:
    #     # If it's not a magnet, and not a command, the bot could ignore it or respond with a help message.
    #     pass

# Download callback handlers (pause, resume, cancel)
@bot.on_callback_query(filters.regex(r"^pause:(.+)"))
async def pause_handler(client, cb):
    dl_id = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if dl_id in active_downloads:
        active_downloads[dl_id]["paused"] = True
        await cb.answer(get_text(uid, "download_paused"))
    else:
        await cb.answer(get_text(uid, "download_not_found"), show_alert=True)

@bot.on_callback_query(filters.regex(r"^resume:(.+)"))
async def resume_handler(client, cb):
    dl_id = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if dl_id in active_downloads:
        active_downloads[dl_id]["paused"] = False
        active_downloads[dl_id]["start"] = time.time()
        await cb.answer(get_text(uid, "resuming_download"))
    else:
        await cb.answer(get_text(uid, "download_not_found"), show_alert=True)

@bot.on_callback_query(filters.regex(r"^cancel:(.+)"))
async def cancel_handler(client, cb):
    dl_id = cb.data.split(":", 1)[1]
    uid = cb.from_user.id
    if dl_id in active_downloads:
        active_downloads[dl_id]["cancelled"] = True
        await cb.answer(get_text(uid, "download_cancelled"))
    else:
        await cb.answer(get_text(uid, "download_not_found"), show_alert=True)

# ——— NEW COMMANDS ——————————————————————————————————————————————

# ——— BOT STARTUP ——————————————————————————————————————————————
if __name__ == "__main__":
     init_db()
     # Start download workers when the bot starts
     bot.start()
     asyncio.get_event_loop().create_task(start_download_workers())
     idle()
     bot.stop()
