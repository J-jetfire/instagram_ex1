import logging

import aiohttp
from asyncio import Lock
from src.custom_exceptions import ProfileIsPrivateException
import requests
from fastapi import HTTPException
import os
from collections import deque
from dotenv import load_dotenv
from celery import Celery

api_logger = logging.getLogger("api_logger")
error_logger = logging.getLogger("errors_logger")

dotenv_path = "./.env"

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

INSTAGRAM_API_KEYS = deque(os.environ.get("INSTAGRAM_API_KEYS").split(','))
INSTAGRAM_API_KEYS_LOCK = Lock()

BROKER_URL = 'redis://redis:6379/2'
BACKEND_URL = 'redis://redis:6379/3'

celery = Celery('instagram', broker=BROKER_URL, backend=BACKEND_URL)


async def is_user_private(username: str) -> bool:
    data = await get_user_profile_info_by_username(username)
    return data.get('is_private', True)


async def check_and_raise_if_private(username: str):
    if await is_user_private(username):
        raise ProfileIsPrivateException(detail="The profile is private")


async def user_data_main(username: str) -> dict:
    user_profile = await get_user_profile_info_by_username(username)

    if not user_profile.get('user_id'):
        error_logger.error("Parameter 'user_id' is null. common_functions.py. 34 row.")
        return {}

    url = f"https://api.com/usercontact/{user_profile.get('user_id')}"
    response = await api_call(url)

    json_data = await safe_json(response)

    if response.status != 200 or not json_data:
        return {}

    data = json_data.get('data', {}).get('user', {})

    return {
        'id': data.get('pk', ''),
        'profile_link': f'https://instagram.com/{username}',
        'username': username,
        'full_name': data.get('full_name', ''),
        'icon_url': data.get('profile_pic_url', ''),
        'is_private': data.get('is_private', True),
        'followers_count': data.get('follower_count', 0),
        'followings_count': data.get('following_count', 0)
    }


async def get_user_profile_info_by_username(username: str) -> dict:
    url = f"https://api.com/userinfo/{username}"
    response = await api_call(url)

    json_data = await safe_json(response)

    if response.status != 200 or not json_data:
        return {}

    data = json_data.get('data', {})
    user_id = data.get('id', '')
    is_private = data.get('is_private', True)

    if not user_id:
        return {}

    return {
        'user_id': user_id,
        'is_private': is_private
    }


async def get_next_api_key():
    """
    INSTAGRAM_API_KEYS_LOCK используется для того, чтобы гарантировать,
    что операции с INSTAGRAM_API_KEYS (например, popleft и append)
    будут происходить атомарно, даже при одновременном доступе нескольких корутин.
    блок async with INSTAGRAM_API_KEYS_LOCK: гарантирует, что только одна корутина может
    находиться в этом блоке в любой момент времени. Это предотвращает ситуации,
    когда две корутины пытаются одновременно изменить INSTAGRAM_API_KEYS,
    что может привести к непредсказуемым результатам.
    """
    async with INSTAGRAM_API_KEYS_LOCK:
        key = INSTAGRAM_API_KEYS.popleft()
        INSTAGRAM_API_KEYS.append(key)
    return key


async def api_call(url):
    current_key = await get_next_api_key()

    headers = {
        "...": current_key,
        "...": "api.com"
    }

    api_logger.info(f"Making a request to {url}. API KEY: {current_key}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return response

    except aiohttp.ClientError as e:
        error_logger.error(f"Failed to make request to {url}. Error: {e}")
        return None


async def safe_json(response):
    """
    Decode JSON from response safely.

    Args:
        response: The response object.

    Returns:
        Parsed JSON data or an empty dictionary in case of failure.
    """
    try:
        return await response.json()  # Используем `await` для асинхронного парсинга
    except:
        error_logger.error("Failed to decode JSON from response: %s", response.text)
        return {}


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.json()
