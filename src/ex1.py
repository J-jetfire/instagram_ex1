import asyncio
import datetime
import logging
from src.custom_exceptions import ProfileIsPrivateException
from src.config import MAX_ANALYSIS_FOLLOWERS_AND_FOLLOWS, MAX_ANALYSIS_LIKES_AND_COMMENTS, MAX_ANALYSIS_POSTS_AND_REELS
from src.common_functions import api_call, safe_json, user_data_main
from cachetools import cached, TTLCache

cache = TTLCache(maxsize=100, ttl=3600)  # кэш с максимальным размером 100 записей и временем жизни 1 час

# Устанавливаем логгер
logger = logging.getLogger(__name__)

"""
Асинхронность выполнена для всех функций.
Нельзя вызывать асинхронную функцию внутри синхронной.

Информация по Кэшу:
декоратор @lru_cache(maxsize=128) кэширует результаты функции согласно указанным параметрам.
maxsize=128 указывает максимальный размер кэша. В данном случае, он ограничен 128 результатами. 
Если кэш заполнен, более старые результаты будут вытеснены новыми.
При `maxsize=None` -> размер кеша будет без лимита
Если uid всегда один и тот же, то ответ всегда будет тот же. 
Кэш будет хранить результаты вызовов функции для этого uid. Если на API что-то изменится, 
кэш будет продолжать выдавать предыдущий результат, так как он кэширован.
Время жизни кэша в данном случае не задано явно (параметр ttl не указан), поэтому результаты будут 
храниться в кэше до его заполнения (maxsize=128). Как только кэш заполнится, старые результаты будут вытеснены новыми.
Таким образом, если API не изменяется и uid остается неизменным, то этот кэш будет хранить результаты 
запросов для этого uid. Если API изменится, но uid останется тем же, кэш все равно будет возвращать 
предыдущие результаты до тех пор, пока они не будут вытеснены новыми вызовами. 
Следовательно, если @lru_cache подходит, и результаты с одинаковыми входными данными будут всегда одинаковые,
то можно использовать @lru_cache. Иначе можно использовать альтернативы, 
в которых можно устанавливать время жизни кэша (cachetools или Redis)
Здесь используем cachetools, однако (ВАЖНО!) нужно учесть насколько нужны актуальные данные 
и как часто будут выполняться запросы, чтобы установить правильное время и количество кэшируемых результатов.
"""


# Сбор данных об отметках пользователя
@cached(cache)
async def user_tagget_count(uid: int):
    """
    Возвращает количество теггированных постов пользователя.

    Args:
        uid (int): Идентификатор пользователя.

    Returns:
        int: Количество теггированных постов.
    """

    url = f"https://instagram-scraper-20231.p.rapidapi.com/usertaggedposts/{uid}/100/%7Bend_cursor%7D"
    response = await api_call(url)  # Используем асинхронную версию api_call

    if response is not None and response.status == 200:
        json_data = await safe_json(response)  # Используем асинхронную версию safe_json

        if json_data:
            data = json_data.get('data', {})
            if isinstance(data, dict) and 'edges' in data:
                return len(data['edges'])

    # Логируем предупреждение, если ответ пустой
    logger.warning("Пустой ответ от API")
    return 0


# Сбор данных о highlights пользователя
@cached(cache)
async def user_highlights_count(uid: int):
    """
    Возвращает количество highlights пользователя.

    Args:
        uid (int): Идентификатор пользователя.

    Returns:
        int: Количество highlights.
    """
    url = f"https://instagram-scraper-20231.p.rapidapi.com/userhighlights/{uid}"
    response = await api_call(url)  # Используем асинхронную версию api_call

    if response is not None and response.status == 200:
        json_data = await safe_json(response)  # Используем асинхронную версию safe_json

        if json_data:
            data = json_data.get('data', {})
            if isinstance(data, dict):  # Проверяем, что data - это словарь
                return len(data)

    # Логируем предупреждение, если ответ пустой
    logger.warning("Пустой ответ от API")
    return 0


# Добавлена функция process_batch, здесь мы не можем использовать покетные запросы
async def process_batch(shortcode, end_cursor, result, counter):
    """
    Обрабатывает батч данных о лайках к посту.

    Args:
        shortcode (str): Короткий код поста.
        end_cursor (str): Курсор для пагинации.
        result (list): Список для хранения данных.
        counter (int): Счетчик.

    Returns:
        str or None: Новый end_cursor или None, если завершено.
    """
    url = f"https://instagram-scraper-20231.p.rapidapi.com/postlikes/{shortcode}/1000/{end_cursor}"
    response = await api_call(url)

    json_data = await safe_json(response)

    if json_data and response.status == 200:
        data = json_data.get('data', {})
        likes = data.get('likes', [])

        for like in likes:
            node = like.get('node', {})
            result.append({
                'username': node.get('username', ''),
                'icon_url': node.get('profile_pic_url', ''),
                'profile_link': f'https://instagram.com/{node.get("username", "")}',
                'id': node.get('id', '')
            })

        # Если условие срабатывает, то происходит новый запрос, иначе процесс завершается.
        if (counter * 50) < MAX_ANALYSIS_LIKES_AND_COMMENTS:
            return data.get('end_cursor')

    return None


async def post_likes(shortcode: str, result: list, end_cursor: str = "%7Bend_cursor%7D", counter: int = 1):
    """
    Получает данные о лайках к посту.

    Args:
        shortcode (str): Короткий код поста.
        result (list): Список для хранения данных.
        end_cursor (str, optional): Курсор для пагинации. Defaults to "%7Bend_cursor%7D".
        counter (int, optional): Счетчик. Defaults to 1.

    Returns:
        int: Общее количество лайков.
    """
    while end_cursor is not None:
        end_cursor = await process_batch(shortcode, end_cursor, result, counter)
        counter += 1
    return len(result)


# TODO: Сделать возможность получения большего количества комментарий, чем один запрос!
# Можно добавить end_cursor и выполнять больше запросов чтобы получить больше комментов
# Сбор детальной информации о комментариях и пользователях кто оставил комментарий
async def post_comments(shortcode: str) -> tuple:
    """
    Получает данные о комментариях к посту.

    Args:
        shortcode (str): Короткий код поста.

    Returns:
        tuple: Список комментариев и общее количество комментариев.
    """
    url = f"https://instagram-scraper-20231.p.rapidapi.com/postcomments/{shortcode}/%7Bend_cursor%7D/%7Bscraperid%7D"
    response = await api_call(url)

    if response is not None and response.status == 200:
        json_data = await safe_json(response)
        result = []

        if json_data:
            data = json_data.get('data', {})
            comments = data.get('comments', [])

            seen_users = set()  # track users who have already commented

            for comment in comments:
                user_name = comment.get('user', {}).get('username', '')

                if user_name not in seen_users:  # only process if user hasn't commented before
                    result.append(await _process_comment(comment))
                    seen_users.add(user_name)

                for child_comment in comment.get('preview_child_comments', []):
                    child_user_name = child_comment.get('user', {}).get('username', '')

                    if child_user_name not in seen_users:  # same check for child comments
                        result.append(await _process_comment(child_comment))
                        seen_users.add(child_user_name)

            return result, data.get('count', 0)
    else:
        # Логируем предупреждение, если ответ пустой
        logger.warning("Пустой ответ от API")
        return [], 0


# Формирование структуры данных для каждого комментария
async def _process_comment(comment: dict) -> dict:
    """
    Формирует структуру данных для комментария.

    Args:
        comment (dict): Данные о комментарии.

    Returns:
        dict: Структура данных комментария.
    """
    user = comment.get('user', {})
    date = datetime.datetime.fromtimestamp(comment.get('created_at_utc', 0))
    return {
        'date': str(date),
        'has_liked_comment': comment.get('has_liked_comment', False),
        'text': comment.get('text', ''),
        'username': user.get('username', ''),
        'icon_url': user.get('profile_pic_url', ''),
        'profile_link': f'https://instagram.com/{user.get("username", "")}',
        'id': user.get('pk_id', '')
    }


# New function here
async def process_batch_follow(full_list, url, counter):
    """
    Обрабатывает батч данных о подписчиках/подписках пользователя.

    Args:
        full_list (list): Список данных.
        url (str): URL для запроса.
        counter (int): Счетчик.

    Returns:
        str or None: Новый end_cursor или None, если завершено.
    """
    response = await api_call(url)
    json_data = await safe_json(response)

    if json_data and response.status == 200:
        data = json_data.get('data', {})
        users = data.get('user', [])

        for user in users:
            full_list.append({
                'username': user.get('username', ''),
                'icon_url': user.get('profile_pic_url', ''),
                'profile_link': f'https://instagram.com/{user.get("username", "")}'
            })

        # Если условие срабатывает, то происходит новый запрос, иначе процесс завершается.
        if (counter * 50) < MAX_ANALYSIS_FOLLOWERS_AND_FOLLOWS:
            return data.get('end_cursor')

    return None


# Сбор данных о подписчиках пользователя - reworked
@cached(cache)
async def user_data_followers(uid: int, full_list: list, offset: int = 0, counter: int = 1):
    """
    Получает данные о подписчиках пользователя.

    Args:
        uid (int): Идентификатор пользователя.
        full_list (list): Список для хранения данных.
        offset (int, optional): Смещение. Defaults to 0.
        counter (int, optional): Счетчик. Defaults to 1.
    """
    while offset is not None:
        url = f"https://instagram-scraper-20231.p.rapidapi.com/userfollowers/{uid}/1000/{offset}"
        offset = await process_batch_follow(full_list, url, counter)
        counter += 1

    return full_list


# Сбор данных о подписках пользователя - reworked
@cached(cache)
async def user_data_following(uid: int, full_list: list, offset: int = 0, counter: int = 1):
    """
    Получает данные о подписках пользователя.

    Args:
        uid (int): Идентификатор пользователя.
        full_list (list): Список для хранения данных.
        offset (int, optional): Смещение. Defaults to 0.
        counter (int, optional): Счетчик. Defaults to 1.
    """
    while offset is not None:
        url = f"https://instagram-scraper-20231.p.rapidapi.com/userfollowing/{uid}/1000/{offset}"
        offset = await process_batch_follow(full_list, url, counter)
        counter += 1

    return full_list


# new func
async def process_batch_posts(url, full_list, total_likes_count, total_comments_count, total_views_count, counter):
    """
    Обрабатывает батч данных о постах пользователя.

    Args:
        url (str): URL для запроса.
        full_list (list): Список данных.
        total_likes_count (int): Общее количество лайков.
        total_comments_count (int): Общее количество комментариев.
        total_views_count (int): Общее количество просмотров.
        counter (int): Счетчик.

    Returns:
        tuple: Новый end_cursor, обновленные счетчики.
    """
    response = await api_call(url)
    json_data = await safe_json(response)

    if json_data and response.status == 200:
        data = json_data.get('data', {})
        edges = data.get('edges', [])

        existing_shortcodes = set(post.get('shortcode') for post in full_list)

        for edge in edges:
            post = edge.get('node', {})

            if post.get('shortcode') in existing_shortcodes:
                continue

            processed_post = _process_post(post)
            full_list.append(processed_post)

            total_likes_count += processed_post.get('likes_count', 0)
            total_comments_count += processed_post.get('comments_count', 0)
            total_views_count += processed_post.get('view_count', 0)

        # Если условие срабатывает, то происходит новый запрос, иначе процесс завершается.
        if (counter * 50) < MAX_ANALYSIS_POSTS_AND_REELS:
            return data.get('end_cursor'), total_likes_count, total_comments_count, total_views_count

    return None, total_likes_count, total_comments_count, total_views_count


# Сбор данных о постах пользователя
@cached(cache)
async def user_data_posts(uid: int, full_list: list, end_cursor: str = '%7Bend_cursor%7D', counter: int = 0,
                          total_views_count: int = 0,
                          total_likes_count: int = 0,
                          total_comments_count: int = 0) -> tuple:
    """
    Получает данные о постах пользователя.

    Args:
        uid (int): Идентификатор пользователя.
        full_list (list): Список для хранения данных.
        end_cursor (str, optional): Курсор для пагинации. Defaults to '%7Bend_cursor%7D'.
        counter (int, optional): Счетчик. Defaults to 0.
        total_views_count (int, optional): Общее количество просмотров. Defaults to 0.
        total_likes_count (int, optional): Общее количество лайков. Defaults to 0.
        total_comments_count (int, optional): Общее количество комментариев. Defaults to 0.

    Returns:
        tuple: Обновленные счетчики.
    """
    while end_cursor is not None:
        url = f"https://instagram-scraper-20231.p.rapidapi.com/userposts/{uid}/1000/{end_cursor}"
        end_cursor, total_likes_count, total_comments_count, total_views_count = await process_batch_posts(
            url, full_list, total_likes_count, total_comments_count, total_views_count, counter
        )
        counter += 1
    return total_likes_count, total_comments_count, total_views_count


# Формирование структуры данных для каждого поста
async def _process_post(post: dict) -> dict:
    """
    Формирует структуру данных для Постов.

    Args:
        post (dict): Данные о Постах.

    Returns:
        dict: Структура данных Поста.
    """
    shortcode = post.get('shortcode', '')
    post_like = []
    likes_count = await post_likes(shortcode, post_like)
    post_comment, comments_count = await post_comments(shortcode)
    date = post.get('taken_at_timestamp') or post.get('taken_at', 0)
    post_date = datetime.datetime.fromtimestamp(date)

    edges = post.get('edge_media_to_caption', {}).get('edges', [])
    if edges:
        text = edges[0].get('node', {}).get('text', '')
    else:
        text = ''

    return {
        'shortcode': shortcode,
        'likes': post_like,
        'comments': post_comment,
        'post_url': f"https://instagram.com/p/{shortcode}",
        'post_date': str(post_date),
        'view_count': post.get('video_view_count', 0) if post.get('is_video', False) else 0,
        'media': [edge.get('node', {}).get('display_url', '') for edge in
                  post.get('edge_sidecar_to_children', {}).get('edges', [])] or [post.get('display_url', '')],
        'likes_count': likes_count,
        'comments_count': comments_count,
        'text': text,
    }


# Сбор данных о Reels пользователя
async def process_batch_reels(url, full_list, total_likes_count, total_comments_count, total_views_count,
                              counter) -> tuple:
    """
    Обрабатывает батч данных о Reels пользователя.

    Args:
        url (str): URL для запроса.
        full_list (list): Список данных.
        total_likes_count (int): Общее количество лайков.
        total_comments_count (int): Общее количество комментариев.
        total_views_count (int): Общее количество просмотров.
        counter (int): Счетчик.

    Returns:
        tuple: Новый end_cursor, обновленные счетчики.
    """
    response = await api_call(url)
    json_data = await safe_json(response)

    if json_data and response.status == 200:

        data = json_data.get('data', {})
        edges = data.get('items', [])

        for edge in edges:
            media = edge.get('media', {})
            processed_post = await _process_reels(media)
            full_list.append(processed_post)

            total_likes_count += processed_post.get('likes_count', 0)
            total_comments_count += processed_post.get('comments_count', 0)
            total_views_count += processed_post.get('view_count', 0)

        # Если условие срабатывает, то происходит новый запрос, иначе процесс завершается.
        if (counter * 50) < MAX_ANALYSIS_POSTS_AND_REELS:
            return data.get('end_cursor'), total_likes_count, total_comments_count, total_views_count

    return None, total_likes_count, total_comments_count, total_views_count


@cached(cache)
async def user_data_reels(uid: int, full_list: list, end_cursor: str = '%7Bend_cursor%7D',
                          counter: int = 0,
                          total_views_count: int = 0,
                          total_likes_count: int = 0,
                          total_comments_count: int = 0) -> tuple:
    """
    Получает данные о Reels пользователя.

    Args:
        uid (int): Идентификатор пользователя.
        full_list (list): Список для хранения данных.
        end_cursor (str, optional): Курсор для пагинации. Defaults to '%7Bend_cursor%7D'.
        counter (int, optional): Счетчик. Defaults to 0.
        total_views_count (int, optional): Общее количество просмотров. Defaults to 0.
        total_likes_count (int, optional): Общее количество лайков. Defaults to 0.
        total_comments_count (int, optional): Общее количество комментариев. Defaults to 0.

    Returns:
        tuple: Обновленные счетчики.
    """
    while end_cursor is not None:
        url = f"https://instagram-scraper-20231.p.rapidapi.com/userreels/{uid}/1000/{end_cursor}"
        end_cursor, total_likes_count, total_comments_count, total_views_count = await process_batch_reels(
            url, full_list, total_likes_count, total_comments_count, total_views_count, counter
        )
        counter += 1
    return total_likes_count, total_comments_count, total_views_count


# Формирование структуры данных для каждого Reels
async def _process_reels(post: dict) -> dict:
    """
    Формирует структуру данных для Reels.

    Args:
        post (dict): Данные о Reels.

    Returns:
        dict: Структура данных Reels.
    """
    shortcode = post.get('code', '')
    post_like = []
    likes_count = await post_likes(shortcode, post_like)
    post_comment, comments_count = await post_comments(shortcode)
    date = post.get('taken_at', 0)
    post_date = datetime.datetime.fromtimestamp(date)

    edges = post.get('caption', {})
    if edges:
        text = edges.get('text', '')
    else:
        text = ''

    return {
        'shortcode': shortcode,
        'likes': post_like,
        'comments': post_comment,
        'post_url': f"https://instagram.com/tv/{shortcode}",
        'post_date': str(post_date),
        'view_count': post.get('play_count', post.get('view_count', 0)),
        'media': [post.get('image_versions2', {}).get("candidates", [{}])[0].get("url", None)],
        'likes_count': likes_count,
        'comments_count': comments_count,
        'text': text,
    }


# Точка входа
@cached(cache)
async def get_analysis_by_single_account(username: str) -> dict:
    result = await user_data_main(username)

    if result.get('is_private', True):
        raise ProfileIsPrivateException(detail="The profile is private")

    uid = result.get('id', '')
    if not uid:
        print("Parameter 'uid' is null. kernel.py. 528 row.")
        return {}
    else:
        uid = int(uid)

    followers_list = []
    following_list = []
    media_list = []

    # Определяем задачи для паралельного использования
    reels_task = asyncio.create_task(user_data_reels(uid, media_list))
    posts_task = asyncio.create_task(user_data_posts(uid, media_list))

    followers_task = asyncio.create_task(user_data_followers(uid, followers_list))
    following_task = asyncio.create_task(user_data_following(uid, following_list))

    tagged_count_task = asyncio.create_task(user_tagget_count(uid))
    highlights_count_task = asyncio.create_task(user_highlights_count(uid))

    # Запускаем все задачи выполняться параллельно
    (reels_likes, reels_comments, reels_views), (posts_likes, posts_comments, posts_views), (tagged_count), (
        highlights_count), *_ = await asyncio.gather(
        reels_task, posts_task, tagged_count_task, highlights_count_task, followers_task, following_task
    )

    # results
    total_reels_likes_count = reels_likes
    total_reels_comments_count = reels_comments
    total_reels_views_count = reels_views
    total_likes_count = posts_likes
    total_comments_count = posts_comments
    total_views_count = posts_views

    media_list = sorted(media_list, key=lambda post: post.get('post_date', ''), reverse=True)

    return {
        'id': uid,
        'profile_link': f'https://instagram.com/{username}',
        'username': username,
        'full_name': result.get('full_name', ''),
        'followers_count': result.get('followers_count', 0),
        'follows_count': result.get('followings_count', 0),
        'icon_url': result.get('icon_url', ''),
        'data': {
            'posts_count': len(media_list),
            'comments_count': total_comments_count + total_reels_comments_count,
            'views_count': total_views_count + total_reels_views_count,
            'likes_count': total_likes_count + total_reels_likes_count,
            'followers': followers_list,
            'following': following_list,
            'posts': media_list,
            'taggets_count': tagged_count,  #
            'highlights_count': highlights_count  #
        }
    }
