import os
import hashlib
import asyncio
import logging
import contextlib
import re
from datetime import datetime
from typing import List, Optional, Dict, Any
from io import BytesIO
from dataclasses import dataclass
from urllib.parse import urlparse
from collections import Counter

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, ContentType,
    PhotoSize, Document, Video,
    Audio, Sticker, Animation,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
import asyncpg
import json
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message as TgMessage
from telethon.tl.functions.messages import ImportChatInviteRequest

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


@dataclass
class ProcessedContent:
    file_hash: str
    file_path: str
    db_id: int
    timestamp: datetime


class ContentCollectorBot:
    def __init__(self, token: str, database_url: str, download_path: str = "./downloads",
                 telethon_api_id: Optional[int] = None,
                 telethon_api_hash: Optional[str] = None,
                 telethon_session: Optional[str] = None,
                 notification_chat_id: Optional[int] = None):
        self.token = token
        self.database_url = database_url
        self.download_path = download_path
        self.telethon_api_id = telethon_api_id
        self.telethon_api_hash = telethon_api_hash
        self.telethon_session = telethon_session
        self.notification_chat_id = notification_chat_id
        self.bot = Bot(
            token=self.token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        self.router = Router()
        self.processed_content: Dict[str, ProcessedContent] = {}
        self.media_groups: Dict[str, List[Message]] = {}
        self.tg_client: Optional[TelegramClient] = None
        self.monitored_channels: Dict[str, Dict[str, Any]] = {}
        self.monitor_task: Optional[asyncio.Task] = None
        self.db_pool = None
        self._setup_download_directory()
        self._setup_handlers()
        self.dp.include_router(self.router)

    def _setup_download_directory(self):
        """Создание директории для загрузок"""
        os.makedirs(self.download_path, exist_ok=True)
        os.makedirs(os.path.join(self.download_path, "text"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path, "image"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path, "video"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path,
                    "document"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path, "audio"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path, "sticker"), exist_ok=True)
        os.makedirs(os.path.join(self.download_path,
                    "animation"), exist_ok=True)
        logger.info(f"Download directory setup: {self.download_path}")

    async def _init_database(self):
        """Инициализация подключения к PostgreSQL"""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10
            )

            # Создание таблиц, если они не существуют
            async with self.db_pool.acquire() as conn:
                # Таблица для текста сообщений
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS "text" (
                        text_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        message_text TEXT NOT NULL
                    )
                ''')

                # Комментарии для таблицы text
                await conn.execute('''
                    COMMENT ON TABLE "text" IS 'Текст сообщения';
                    COMMENT ON COLUMN "text".text_id IS 'Уникальный идентификатор текста сообщения';
                    COMMENT ON COLUMN "text".message_text IS 'Текст сообщения';
                ''')

                # Таблица для фото
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS photo (
                        photo_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        photo_link VARCHAR(250) NOT NULL CHECK(LENGTH(photo_link) > 3)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE photo IS 'Фото сообщения';
                    COMMENT ON COLUMN photo.photo_id IS 'Уникальный идентификатор фото сообщения';
                    COMMENT ON COLUMN photo.photo_link IS 'Фото сообщения';
                ''')

                # Таблица для аудио
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS audio (
                        audio_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        audio_link VARCHAR(250) NOT NULL CHECK(LENGTH(audio_link) > 3)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE audio IS 'Аудио сообщения';
                    COMMENT ON COLUMN audio.audio_id IS 'Уникальный идентификатор аудио сообщения';
                    COMMENT ON COLUMN audio.audio_link IS 'Аудио сообщения';
                ''')

                # Таблица для видео
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS video (
                        video_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        video_link VARCHAR(250) NOT NULL CHECK(LENGTH(video_link) > 3)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE video IS 'Видео сообщения';
                    COMMENT ON COLUMN video.video_id IS 'Уникальный идентификатор видео сообщения';
                    COMMENT ON COLUMN video.video_link IS 'Видео сообщения';
                ''')

                # Таблица для документов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS document (
                        document_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        document_link VARCHAR(250) NOT NULL CHECK(LENGTH(document_link) > 3),
                        original_name VARCHAR(250)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE document IS 'Документ сообщения';
                    COMMENT ON COLUMN document.document_id IS 'Уникальный идентификатор документа сообщения';
                    COMMENT ON COLUMN document.document_link IS 'Документ сообщения';
                    COMMENT ON COLUMN document.original_name IS 'Оригинальное имя файла';
                ''')

                # Таблица для стикеров
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS sticker (
                        sticker_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        sticker_link VARCHAR(250) NOT NULL CHECK(LENGTH(sticker_link) > 3)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE sticker IS 'Стикер сообщения';
                    COMMENT ON COLUMN sticker.sticker_id IS 'Уникальный идентификатор стикера сообщения';
                    COMMENT ON COLUMN sticker.sticker_link IS 'Стикер сообщения';
                ''')

                # Таблица для анимаций
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS animation (
                        animation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        animation_link VARCHAR(250) NOT NULL CHECK(LENGTH(animation_link) > 3)
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE animation IS 'Анимация сообщения';
                    COMMENT ON COLUMN animation.animation_id IS 'Уникальный идентификатор анимации сообщения';
                    COMMENT ON COLUMN animation.animation_link IS 'Анимация сообщения';
                ''')

                # Таблица для ссылок
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS links (
                        link_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        url VARCHAR(500) NOT NULL CHECK(LENGTH(url) > 3),
                        domain VARCHAR(100),
                        title VARCHAR(200),
                        description TEXT
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE links IS 'Ссылки из сообщений';
                    COMMENT ON COLUMN links.link_id IS 'Уникальный идентификатор ссылки';
                    COMMENT ON COLUMN links.url IS 'URL ссылки';
                    COMMENT ON COLUMN links.domain IS 'Домен ссылки';
                    COMMENT ON COLUMN links.title IS 'Заголовок ссылки (если доступен)';
                    COMMENT ON COLUMN links.description IS 'Описание ссылки (если доступно)';
                ''')

                # Таблица для каналов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS channel (
                        channel_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        tag_channel VARCHAR(50) NOT NULL CHECK(LENGTH(tag_channel) > 3),
                        name_channel VARCHAR(50) NOT NULL CHECK(LENGTH(name_channel) > 3),
                        is_active BOOLEAN DEFAULT FALSE,
                        added_by BIGINT,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_message_id BIGINT DEFAULT 0,
                        last_check_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                await conn.execute('''
                    COMMENT ON TABLE channel IS 'Телеграмм канал';
                    COMMENT ON COLUMN channel.channel_id IS 'Уникальный идентификатор телеграмм канала';
                    COMMENT ON COLUMN channel.tag_channel IS 'Тэг телеграмм канала';
                    COMMENT ON COLUMN channel.name_channel IS 'Название телеграмм канала';
                ''')

                # Таблица для сообщений каналов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS message_channel (
                        message_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        channel_id BIGINT NOT NULL REFERENCES channel(channel_id),
                        creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        text_id BIGINT REFERENCES "text"(text_id),
                        photo_id BIGINT REFERENCES photo(photo_id),
                        video_id BIGINT REFERENCES video(video_id),
                        audio_id BIGINT REFERENCES audio(audio_id),
                        document_id BIGINT REFERENCES document(document_id),
                        sticker_id BIGINT REFERENCES sticker(sticker_id),
                        animation_id BIGINT REFERENCES animation(animation_id),
                        link_id BIGINT REFERENCES links(link_id),
                        telegram_message_id BIGINT,
                        file_hash VARCHAR(64) UNIQUE
                    )
                ''')

                # Добавляем недостающие столбцы, если таблица уже существует
                await conn.execute('''
                    DO $$ 
                    BEGIN
                        -- Добавляем document_id если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'document_id') THEN
                            ALTER TABLE message_channel ADD COLUMN document_id BIGINT REFERENCES document(document_id);
                        END IF;
                        
                        -- Добавляем sticker_id если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'sticker_id') THEN
                            ALTER TABLE message_channel ADD COLUMN sticker_id BIGINT REFERENCES sticker(sticker_id);
                        END IF;
                        
                        -- Добавляем animation_id если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'animation_id') THEN
                            ALTER TABLE message_channel ADD COLUMN animation_id BIGINT REFERENCES animation(animation_id);
                        END IF;
                        
                        -- Добавляем telegram_message_id если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'telegram_message_id') THEN
                            ALTER TABLE message_channel ADD COLUMN telegram_message_id BIGINT;
                        END IF;
                        
                        -- Добавляем file_hash если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'file_hash') THEN
                            ALTER TABLE message_channel ADD COLUMN file_hash VARCHAR(64) UNIQUE;
                        END IF;
                        
                        -- Добавляем link_id если не существует
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                     WHERE table_name = 'message_channel' AND column_name = 'link_id') THEN
                            ALTER TABLE message_channel ADD COLUMN link_id BIGINT REFERENCES links(link_id);
                        END IF;
                    END $$;
                ''')

                await conn.execute('''
                    COMMENT ON TABLE message_channel IS 'Сообщения телеграмм каналов - связующая таблица между каналами и контентом';
                    COMMENT ON COLUMN message_channel.message_id IS 'Уникальный идентификатор сообщения канала в нашей БД (автоинкремент)';
                    COMMENT ON COLUMN message_channel.channel_id IS 'Ссылка на канал из таблицы channel - к какому каналу относится сообщение';
                    COMMENT ON COLUMN message_channel.creation_time IS 'Дата и время создания/появления сообщения в Telegram (из API)';
                    COMMENT ON COLUMN message_channel.text_id IS 'Ссылка на текст сообщения из таблицы text (если есть текст)';
                    COMMENT ON COLUMN message_channel.photo_id IS 'Ссылка на фото из таблицы photo (если есть фото)';
                    COMMENT ON COLUMN message_channel.video_id IS 'Ссылка на видео из таблицы video (если есть видео)';
                    COMMENT ON COLUMN message_channel.audio_id IS 'Ссылка на аудио из таблицы audio (если есть аудио)';
                    COMMENT ON COLUMN message_channel.document_id IS 'Ссылка на документ из таблицы document (если есть документ)';
                    COMMENT ON COLUMN message_channel.sticker_id IS 'Ссылка на стикер из таблицы sticker (если есть стикер)';
                    COMMENT ON COLUMN message_channel.animation_id IS 'Ссылка на анимацию из таблицы animation (если есть анимация)';
                    COMMENT ON COLUMN message_channel.link_id IS 'Ссылка на ссылку из таблицы links (если есть ссылки)';
                    COMMENT ON COLUMN message_channel.telegram_message_id IS 'ID сообщения в Telegram API - для связи с оригинальным сообщением, получения обновлений, дедупликации';
                    COMMENT ON COLUMN message_channel.file_hash IS 'MD5 хеш файла для предотвращения дублирования одинакового контента';
                ''')

                # Индексы для оптимизации
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_channel_tag ON channel(tag_channel);
                    CREATE INDEX IF NOT EXISTS idx_channel_active ON channel(is_active);
                    CREATE INDEX IF NOT EXISTS idx_message_channel_id ON message_channel(channel_id);
                    CREATE INDEX IF NOT EXISTS idx_message_creation_time ON message_channel(creation_time);
                    CREATE INDEX IF NOT EXISTS idx_message_file_hash ON message_channel(file_hash);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_links_url ON links(url);
                    CREATE INDEX IF NOT EXISTS idx_links_domain ON links(domain);
                ''')

            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def _init_telethon(self):
        """Инициализация Telethon клиента (опционально, для чтения истории каналов)."""
        if not (self.telethon_api_id and self.telethon_api_hash and self.telethon_session):
            logger.info(
                "Telethon credentials not provided; /fetch will be unavailable")
            return
        try:
            self.tg_client = TelegramClient(StringSession(
                self.telethon_session), self.telethon_api_id, self.telethon_api_hash)
            await self.tg_client.connect()
            if not await self.tg_client.is_user_authorized():
                logger.warning(
                    "Telethon client is not authorized. Provide a valid session string.")
            # disallow bot sessions for monitoring: they cannot use GetHistoryRequest
            me = await self.tg_client.get_me()
            if getattr(me, 'bot', False):
                logger.error(
                    "Telethon session belongs to a bot account; history access is restricted by Telegram.")
                await self.tg_client.disconnect()
                self.tg_client = None
        except Exception as e:
            logger.error(f"Failed to initialize Telethon client: {e}")
            self.tg_client = None

    async def _save_telethon_message(self, msg: TgMessage, channel_name: str = None, channel_id: int = None) -> tuple[bool, list[str]]:
        """Сохранение одного сообщения из Telethon (текст/медиа)."""
        try:
            saved_any = False
            message_processed = False  # Флаг для отслеживания обработки сообщения
            message_date = getattr(msg, 'date', None)
            message_id = getattr(msg, 'id', None)
            chat_id = getattr(msg, 'chat_id', None)

            # Получаем channel_id из базы данных (если не передан как параметр)
            if channel_id is None and self.db_pool and channel_name:
                async with self.db_pool.acquire() as conn:
                    result = await conn.fetchrow('''
                        SELECT channel_id FROM channel WHERE tag_channel = $1
                    ''', channel_name)
                    if result:
                        channel_id = result['channel_id']

            # Переменные для хранения ID контента
            text_id = None
            photo_id = None
            video_id = None
            audio_id = None
            document_id = None
            sticker_id = None
            animation_id = None
            link_id = None
            file_hash = None

            if getattr(msg, 'message', None):
                message_processed = True  # Сообщение содержит текст
                text_bytes = msg.message.encode('utf-8')
                filename = self._generate_filename(None, 'text')
                file_path = self._save_file_locally(
                    text_bytes, filename, 'text')
                if file_path:
                    file_hash = self._calculate_file_hash(text_bytes)
                    if not await self._is_duplicate(file_hash):
                        # Сохраняем текст в базу данных
                        if self.db_pool and channel_id:
                            async with self.db_pool.acquire() as conn:
                                # Сохраняем текст
                                text_result = await conn.fetchrow('''
                                    INSERT INTO "text" (message_text)
                                    VALUES ($1)
                                    RETURNING text_id
                                ''', msg.message)
                                text_id = text_result['text_id']

                                # Извлекаем и сохраняем ссылки
                                links = self._extract_links(msg.message)
                                if links:
                                    # Сохраняем первую ссылку (можно расширить для множественных ссылок)
                                    link_data = links[0]
                                    link_result = await conn.fetchrow('''
                                        INSERT INTO links (url, domain, title, description)
                                        VALUES ($1, $2, $3, $4)
                                        ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
                                        RETURNING link_id
                                    ''', link_data['url'], link_data['domain'], link_data['title'], link_data['description'])
                                    link_id = link_result['link_id']
                                    logger.info(
                                        f"Saved link to DB: {link_data['url']}")

                                logger.info(
                                    f"Saved text to DB: text_id={text_id}")
                        else:
                            logger.warning(
                                f"Cannot save text to DB: channel_id={channel_id}, db_pool={self.db_pool is not None}")

                        self.processed_content[file_hash] = ProcessedContent(
                            file_hash=file_hash,
                            file_path=file_path,
                            db_id=-1,
                            timestamp=datetime.now()
                        )
                        saved_any = True

            if getattr(msg, 'media', None):
                message_processed = True  # Сообщение содержит медиа
                bio = BytesIO()
                if self.tg_client:
                    try:
                        await self.tg_client.download_media(msg, file=bio)
                        data = bio.getvalue()
                        if data:
                            file_hash = self._calculate_file_hash(data)
                            if not await self._is_duplicate(file_hash):
                                # Try to infer content type
                                content_type = 'document'
                                if msg.photo:
                                    content_type = 'image'
                                elif msg.video:
                                    content_type = 'video'
                                elif msg.audio:
                                    content_type = 'audio'
                                elif msg.sticker:
                                    content_type = 'sticker'
                                elif msg.gif or getattr(msg, 'animation', None):
                                    content_type = 'animation'

                                filename = self._generate_filename(
                                    None, content_type)
                                file_path = self._save_file_locally(
                                    data, filename, content_type)
                                if file_path:
                                    # Сохраняем в базу данных
                                    if self.db_pool and channel_id:
                                        async with self.db_pool.acquire() as conn:
                                            # Определяем тип медиа и сохраняем в соответствующую таблицу
                                            media_id = None
                                            if content_type == 'image':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO photo (photo_link)
                                                    VALUES ($1)
                                                    RETURNING photo_id
                                                ''', file_path)
                                                media_id = result['photo_id']
                                                photo_id = media_id
                                                logger.info(
                                                    f"Saved photo to DB: photo_id={photo_id}")
                                            elif content_type == 'video':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO video (video_link)
                                                    VALUES ($1)
                                                    RETURNING video_id
                                                ''', file_path)
                                                media_id = result['video_id']
                                                video_id = media_id
                                                logger.info(
                                                    f"Saved video to DB: video_id={video_id}")
                                            elif content_type == 'audio':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO audio (audio_link)
                                                    VALUES ($1)
                                                    RETURNING audio_id
                                                ''', file_path)
                                                media_id = result['audio_id']
                                                audio_id = media_id
                                                logger.info(
                                                    f"Saved audio to DB: audio_id={audio_id}")
                                            elif content_type == 'document':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO document (document_link, original_name)
                                                    VALUES ($1, $2)
                                                    RETURNING document_id
                                                ''', file_path, filename)
                                                media_id = result['document_id']
                                                document_id = media_id
                                                logger.info(
                                                    f"Saved document to DB: document_id={document_id}")
                                            elif content_type == 'sticker':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO sticker (sticker_link)
                                                    VALUES ($1)
                                                    RETURNING sticker_id
                                                ''', file_path)
                                                media_id = result['sticker_id']
                                                sticker_id = media_id
                                                logger.info(
                                                    f"Saved sticker to DB: sticker_id={sticker_id}")
                                            elif content_type == 'animation':
                                                result = await conn.fetchrow('''
                                                    INSERT INTO animation (animation_link)
                                                    VALUES ($1)
                                                    RETURNING animation_id
                                                ''', file_path)
                                                media_id = result['animation_id']
                                                animation_id = media_id
                                                logger.info(
                                                    f"Saved animation to DB: animation_id={animation_id}")
                                    else:
                                        logger.warning(
                                            f"Cannot save media to DB: channel_id={channel_id}, db_pool={self.db_pool is not None}")

                                    self.processed_content[file_hash] = ProcessedContent(
                                        file_hash=file_hash,
                                        file_path=file_path,
                                        db_id=-1,
                                        timestamp=datetime.now()
                                    )
                                    saved_any = True
                    except Exception as e:
                        logger.error(
                            f"Failed to download media via Telethon: {e}")

            # Если сообщение не содержит ни текста, ни медиа, но имеет ID, считаем его обработанным
            if not message_processed and message_id:
                message_processed = True
                logger.info(
                    f"Processed empty message {message_id} from {channel_name}")

            # Создаем единую запись в message_channel для всех типов контента
            if self.db_pool and channel_id and message_id:
                try:
                    async with self.db_pool.acquire() as conn:
                        # Проверяем, не существует ли уже запись с таким telegram_message_id
                        existing = await conn.fetchval('''
                            SELECT message_id FROM message_channel 
                            WHERE telegram_message_id = $1 AND channel_id = $2
                        ''', message_id, channel_id)

                        if not existing:
                            # Создаем единую запись в message_channel со всеми типами контента
                            creation_time = self._normalize_datetime(
                                message_date)

                            await conn.execute('''
                                INSERT INTO message_channel 
                                (channel_id, text_id, photo_id, video_id, audio_id, document_id, 
                                 sticker_id, animation_id, link_id, telegram_message_id, file_hash, creation_time)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            ''', channel_id, text_id, photo_id, video_id, audio_id, document_id,
                                               sticker_id, animation_id, link_id, message_id, file_hash, creation_time)

                            logger.info(
                                f"Created unified message_channel record: channel_id={channel_id}, telegram_id={message_id}, text_id={text_id}, photo_id={photo_id}, video_id={video_id}")
                            saved_any = True
                        else:
                            logger.info(
                                f"Message_channel record already exists: telegram_id={message_id}")
                except Exception as e:
                    logger.error(
                        f"Failed to create message_channel record: {e}")

            # Собираем типы сохраненного контента
            types_saved = []
            if text_id:
                types_saved.append('text')
            if photo_id:
                types_saved.append('photo')
            if video_id:
                types_saved.append('video')
            if audio_id:
                types_saved.append('audio')
            if document_id:
                types_saved.append('document')
            if sticker_id:
                types_saved.append('sticker')
            if animation_id:
                types_saved.append('animation')
            if link_id:
                types_saved.append('link')

            # Возвращаем True если сообщение было обработано (содержит текст или медиа)
            # или если что-то было сохранено
            return message_processed or saved_any, types_saved
        except Exception as e:
            logger.error(f"Error saving Telethon message: {e}")
            return False, []

    async def fetch_last_messages(self, channel: str, limit: int = 2) -> int:
        """Скачать последние N сообщений из публичного канала (через Telethon)."""
        if not self.tg_client:
            return 0
        try:
            entity = await self.tg_client.get_entity(channel)
            count_saved = 0

            # Получаем channel_id из базы данных или создаем запись для статистики
            channel_id = None
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    # Проверяем, существует ли канал
                    result = await conn.fetchrow('''
                        SELECT channel_id FROM channel WHERE tag_channel = $1
                    ''', channel)

                    if result:
                        channel_id = result['channel_id']
                    else:
                        # Создаем запись канала только для статистики (is_active = False)
                        channel_id = await conn.fetchval('''
                            INSERT INTO channel (tag_channel, name_channel, is_active, added_by, added_at)
                            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                            RETURNING channel_id
                        ''', channel, channel, False, 0)
                        logger.info(
                            f"Created channel record for fetch statistics: {channel}")

            if channel_id is None:
                logger.error(
                    f"Cannot save messages to DB: channel_id is None for {channel}")
                return 0

            async for msg in self.tg_client.iter_messages(entity, limit=limit):
                logger.info(
                    f"Processing message {msg.id} from {channel}, channel_id={channel_id}")
                success, _ = await self._save_telethon_message(msg, channel, channel_id)
                if success:
                    count_saved += 1
                    logger.info(
                        f"Successfully saved message {msg.id}, count_saved={count_saved}")
                else:
                    logger.warning(
                        f"Failed to save message {msg.id} from {channel}")

            return count_saved
        except Exception as e:
            logger.error(f"Failed to fetch messages from {channel}: {e}")
            return 0

    async def _monitor_loop(self):
        """Фоновая задача: опрашивает активные каналы и сохраняет новые сообщения в реальном времени."""
        logger.info("Monitor loop started")
        while True:
            try:
                if self.tg_client and self.monitored_channels:
                    for channel, meta in list(self.monitored_channels.items()):
                        if not meta.get('is_active', True):
                            continue
                        try:
                            entity = await self.tg_client.get_entity(channel)
                        except Exception as e:
                            logger.error(f"Cannot resolve {channel}: {e}")
                            continue

                        last_id = meta.get('last_id', 0)
                        new_count = 0
                        content_counter = Counter()
                        async for msg in self.tg_client.iter_messages(entity, min_id=last_id):
                            # iter_messages yields newest->oldest, but min_id filters strictly > last_id
                            success, types_saved = await self._save_telethon_message(msg, channel, meta.get('channel_id'))
                            if success:
                                new_count += 1
                                content_counter.update(types_saved)
                                if msg.id and msg.id > meta.get('last_id', 0):
                                    meta['last_id'] = msg.id
                                    # Обновляем last_message_id в базе данных
                                    if self.db_pool:
                                        async with self.db_pool.acquire() as conn:
                                            await conn.execute('''
                                                UPDATE channel
                                                SET last_message_id = $1, last_check_at = CURRENT_TIMESTAMP
                                                WHERE tag_channel = $2
                                            ''', msg.id, channel)
                        if new_count:
                            logger.info(
                                f"Fetched {new_count} new messages from {channel}")
                            if self.notification_chat_id:
                                types_str = ', '.join(
                                    [f"{count} {typ}{'s' if count > 1 else ''}" for typ, count in content_counter.items()])
                                total_content = sum(content_counter.values())
                                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                msg_text = f"Saved {new_count} new post{'s' if new_count > 1 else ''} from {channel} at {now}: {types_str} (total content items: {total_content})"
                                try:
                                    await self.bot.send_message(self.notification_chat_id, msg_text)
                                except Exception as e:
                                    logger.error(
                                        f"Failed to send notification: {e}")

                        # Обновляем last_check_at даже если нет новых сообщений
                        if self.db_pool:
                            async with self.db_pool.acquire() as conn:
                                await conn.execute('''
                                    UPDATE channel
                                    SET last_check_at = CURRENT_TIMESTAMP
                                    WHERE tag_channel = $1
                                ''', channel)

                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                logger.info("Monitor loop cancelled")
                break
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                await asyncio.sleep(5.0)

    async def _add_channel(self, channel: str, added_by: int = 0) -> bool:
        """Добавить канал в мониторинг (Telethon)."""
        if not self.tg_client:
            return False
        try:
            entity = await self.tg_client.get_entity(channel)
            telegram_channel_id = getattr(entity, 'id', None)
            channel_title = getattr(entity, 'title', channel)

            # Инициализируем last_id текущим последним сообщением, чтобы не тащить старую историю
            last = None
            async for msg in self.tg_client.iter_messages(entity, limit=1):
                last = msg
                break
            last_id = getattr(last, 'id', 0)

            # Сохраняем в базу данных
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    # Проверяем, существует ли канал
                    existing = await conn.fetchrow('''
                        SELECT channel_id FROM channel WHERE tag_channel = $1
                    ''', channel)

                    if existing:
                        # Обновляем существующий канал
                        await conn.execute('''
                            UPDATE channel SET 
                                is_active = TRUE, 
                                added_by = $2, 
                                last_message_id = $3, 
                                last_check_at = CURRENT_TIMESTAMP
                            WHERE tag_channel = $1
                        ''', channel, added_by, last_id)
                        channel_id = existing['channel_id']
                    else:
                        # Создаем новый канал
                        result = await conn.fetchrow('''
                            INSERT INTO channel (tag_channel, name_channel, is_active, added_by, last_message_id, last_check_at)
                            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                            RETURNING channel_id
                        ''', channel, channel_title, True, added_by, last_id)
                        channel_id = result['channel_id']

            self.monitored_channels[channel] = {
                'is_active': True,
                'last_id': last_id,
                'channel_id': channel_id
            }
            return True
        except Exception as e:
            logger.error(f"Failed to add channel {channel}: {e}")
            return False

    async def _stop_channel(self, channel: str) -> bool:
        """Остановить мониторинг канала."""
        # Обновляем в базе данных
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                result = await conn.execute('''
                    UPDATE channel
                    SET is_active = FALSE, last_check_at = CURRENT_TIMESTAMP
                    WHERE tag_channel = $1
                ''', channel)
                if result == "UPDATE 0":
                    return False

        if channel in self.monitored_channels:
            self.monitored_channels[channel]['is_active'] = False
        return True

    def _main_keyboard(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="/start"), KeyboardButton(text="/list")],
                [KeyboardButton(text="/stats")],
            ],
            resize_keyboard=True
        )

    def _list_inline_keyboard(self) -> InlineKeyboardMarkup:
        buttons = []
        for channel, meta in self.monitored_channels.items():
            state = "🟢" if meta.get('is_active', False) else "🔴"
            buttons.append([InlineKeyboardButton(text=f"{state} {channel}", callback_data=f"noop|{channel}"),
                            InlineKeyboardButton(text="⏹ Stop", callback_data=f"stop|{channel}")])
        if not buttons:
            buttons = [[InlineKeyboardButton(
                text="Нет каналов", callback_data="noop")]]
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    # Отслеживание каналов удалено — обрабатываем все сообщения

    # Добавление/удаление каналов удалено

    # Добавление/удаление каналов удалено

    # Получение списка отслеживаемых каналов удалено

    def _calculate_file_hash(self, file_data: bytes) -> str:
        """Вычисление хеша файла для дедупликации"""
        return hashlib.md5(file_data).hexdigest()

    def _extract_links(self, text: str) -> List[Dict[str, str]]:
        """Извлечение ссылок из текста сообщения"""
        if not text:
            return []

        # Регулярное выражение для поиска URL
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)

        links = []
        for url in urls:
            try:
                parsed = urlparse(url)
                domain = parsed.netloc
                links.append({
                    'url': url,
                    'domain': domain,
                    'title': None,  # Можно добавить парсинг заголовка в будущем
                    'description': None
                })
            except Exception as e:
                logger.warning(f"Failed to parse URL {url}: {e}")
                continue

        return links

    def _normalize_datetime(self, dt):
        """Нормализация datetime - убирает timezone info если есть"""
        if dt is None:
            return datetime.now()
        if dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt

    async def _download_file(self, file_id: str) -> Optional[bytes]:
        """Скачивание файла через Telegram API"""
        try:
            file_info = await self.bot.get_file(file_id)
            file_data = await self.bot.download_file(file_info.file_path)
            return file_data.read()
        except Exception as e:
            logger.error(f"Error downloading file {file_id}: {e}")
            return None

    def _save_file_locally(self, file_data: bytes, filename: str, content_type: str) -> Optional[str]:
        """Сохранение файла локально"""
        try:
            # Создаем путь к файлу
            file_dir = os.path.join(self.download_path, content_type)
            file_path = os.path.join(file_dir, filename)

            # Сохраняем файл
            with open(file_path, 'wb') as f:
                f.write(file_data)

            logger.info(f"File saved locally: {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error saving file locally: {e}")
            return None

    def _generate_filename(self, original_name=None, content_type: str = 'unknown') -> str:
        """Генерация уникального имени файла"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        if original_name:
            extension = original_name.split(
                '.')[-1] if '.' in original_name else 'bin'
            return f"{content_type}_{timestamp}.{extension}"
        return f"{content_type}_{timestamp}"

    async def _is_duplicate(self, file_hash: str) -> bool:
        """Проверка на дубликат (только в памяти)"""
        return file_hash in self.processed_content

    # Сохранение в базу данных удалено

    async def _process_single_file(self, file_id: str, content_type: str,
                                   original_name: str, message: Message) -> bool:
        """Обработка одного файла"""
        # Скачивание файла
        file_data = await self._download_file(file_id)
        if not file_data:
            return False

        # Дедупликация
        file_hash = self._calculate_file_hash(file_data)
        if await self._is_duplicate(file_hash):
            logger.info("Duplicate file detected, skipping...")
            return False

        # Генерация имени файла
        filename = self._generate_filename(original_name, content_type)

        # Сохранение локально
        file_path = self._save_file_locally(file_data, filename, content_type)

        if file_path:
            self.processed_content[file_hash] = ProcessedContent(
                file_hash=file_hash,
                file_path=file_path,
                db_id=-1,
                timestamp=datetime.now()
            )
            logger.info(f"File processed successfully: {file_path}")
            return True
        return False

    async def _process_text(self, message: Message):
        """Обработка текстовых сообщений"""
        if not message.text:
            return False

        text_data = message.text.encode('utf-8')
        filename = self._generate_filename(None, 'text')
        file_path = self._save_file_locally(text_data, filename, 'text')

        if file_path:
            file_hash = self._calculate_file_hash(text_data)
            if not await self._is_duplicate(file_hash):
                self.processed_content[file_hash] = ProcessedContent(
                    file_hash=file_hash,
                    file_path=file_path,
                    db_id=-1,
                    timestamp=datetime.now()
                )
                logger.info(f"Text saved: {file_path}")
                return True
        return False

    async def _process_photo(self, photos: List[PhotoSize], message: Message):
        """Обработка фотографий (берем фото с максимальным качеством)"""
        if not photos:
            return False

        # Берем фото с максимальным размером
        largest_photo = max(photos, key=lambda p: p.file_size or 0)
        return await self._process_single_file(
            largest_photo.file_id, 'image',
            f"photo_{largest_photo.file_id}.jpg", message
        )

    async def _process_document(self, document: Document, message: Message):
        """Обработка документов"""
        return await self._process_single_file(
            document.file_id, 'document', document.file_name, message
        )

    async def _process_video(self, video: Video, message: Message):
        """Обработка видео"""
        return await self._process_single_file(
            video.file_id, 'video', video.file_name, message
        )

    async def _process_audio(self, audio: Audio, message: Message):
        """Обработка аудио"""
        filename = audio.file_name if audio.file_name else f"audio_{audio.file_id}.mp3"
        return await self._process_single_file(
            audio.file_id, 'audio', filename, message
        )

    async def _process_sticker(self, sticker: Sticker, message: Message):
        """Обработка стикеров"""
        return await self._process_single_file(
            sticker.file_id, 'sticker', f"sticker_{sticker.file_id}.webp", message
        )

    async def _process_animation(self, animation: Animation, message: Message):
        """Обработка GIF/анимаций"""
        filename = animation.file_name if animation.file_name else f"animation_{animation.file_id}.gif"
        return await self._process_single_file(
            animation.file_id, 'animation', filename, message
        )

    async def _process_media_group(self, messages: List[Message]):
        """Обработка медиа-группы"""
        logger.info(f"Processing media group with {len(messages)} items")

        for message in messages:
            await self._process_message_content(message)

        # Очистка обработанной медиа-группы
        if messages and messages[0].media_group_id:
            media_group_id = messages[0].media_group_id
            if media_group_id in self.media_groups:
                del self.media_groups[media_group_id]

    async def _process_message_content(self, message: Message):
        """Обработка контента сообщения"""
        try:
            # Текст
            if message.content_type == ContentType.TEXT:
                await self._process_text(message)

            # Фото
            elif message.content_type == ContentType.PHOTO:
                await self._process_photo(message.photo, message)

            # Документы
            elif message.content_type == ContentType.DOCUMENT:
                await self._process_document(message.document, message)

            # Видео
            elif message.content_type == ContentType.VIDEO:
                await self._process_video(message.video, message)

            # Аудио
            elif message.content_type == ContentType.AUDIO:
                await self._process_audio(message.audio, message)

            # Стикер
            elif message.content_type == ContentType.STICKER:
                await self._process_sticker(message.sticker, message)

            # Анимация (GIF)
            elif message.content_type == ContentType.ANIMATION:
                await self._process_animation(message.animation, message)

        except Exception as e:
            logger.error(f"Error processing message content: {e}")

    async def _handle_media_group(self, message: Message):
        """Обработка медиа-групп (сообщений с несколькими медиа)"""
        if not message.media_group_id:
            return False

        # Добавляем сообщение в буфер медиа-группы
        media_group_id = message.media_group_id

        if media_group_id not in self.media_groups:
            self.media_groups[media_group_id] = []

        self.media_groups[media_group_id].append(message)

        # Запускаем таймер для обработки группы через 2 секунды
        # (ожидаем завершения получения всех сообщений группы)
        async def process_group_later():
            await asyncio.sleep(2.0)
            if media_group_id in self.media_groups:
                await self._process_media_group(self.media_groups[media_group_id])

        asyncio.create_task(process_group_later())
        return True

    def _setup_handlers(self):
        """Настройка обработчиков сообщений и команд"""

        @self.router.message(Command("start"))
        async def cmd_start(message: Message):
            """Команда /start - показывает список команд"""
            help_text = (
                "🤖 <b>Content Collector Bot</b>\n\n"
                "Бот сохраняет любой полученный текст и медиа локально и в базу данных.\n"
                "Команды:\n"
                "/start — показать это сообщение.\n"
                "/list — список отслеживаемых каналов из БД.\n"
                "/collect <code>@channel</code> — начать сбор по каналу (Telethon).\n"
                "/stop <code>@channel</code> — остановить сбор по каналу.\n"
                "/fetch <code>@channel</code> — единоразово скачать последние 2 сообщения (Telethon).\n"
                "/stats — статистика сохраненных данных."
            )
            await message.answer(help_text, parse_mode=ParseMode.HTML, reply_markup=self._main_keyboard())

        @self.router.message(Command("list"))
        async def cmd_list(message: Message):
            # Загружаем каналы из базы данных
            channels = []
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch('''
                        SELECT tag_channel, name_channel, is_active, added_at, last_check_at, last_message_id
                        FROM channel
                        WHERE is_active = TRUE
                        ORDER BY added_at DESC
                    ''')
                    channels = [dict(row) for row in rows]

            if not channels:
                await message.answer("📭 Нет отслеживаемых каналов")
                return

            text = "📋 <b>Отслеживаемые каналы</b>\n\n"
            for ch in channels:
                status = "🟢 активен" if ch['is_active'] else "🔴 остановлен"
                added = ch['added_at'].strftime(
                    '%d.%m.%Y %H:%M') if ch['added_at'] else 'неизвестно'
                last_check = ch['last_check_at'].strftime(
                    '%d.%m.%Y %H:%M') if ch['last_check_at'] else 'никогда'
                text += f"• {ch['tag_channel']} ({ch['name_channel']}) — {status}\n"
                text += f"  Добавлен: {added}\n"
                text += f"  Последняя проверка: {last_check}\n"
                if ch['last_message_id']:
                    text += f"  Последнее сообщение ID: {ch['last_message_id']}\n"
                text += "\n"

            await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=self._list_inline_keyboard())

        @self.router.message(Command("stats"))
        async def cmd_stats(message: Message):
            """Показать статистику сохраненных данных"""
            if not self.db_pool:
                await message.answer("❌ База данных не подключена")
                return

            try:
                async with self.db_pool.acquire() as conn:
                    # Статистика каналов
                    channels_count = await conn.fetchval('SELECT COUNT(*) FROM channel')
                    active_channels = await conn.fetchval('SELECT COUNT(*) FROM channel WHERE is_active = TRUE')

                    # Статистика сообщений
                    messages_count = await conn.fetchval('SELECT COUNT(*) FROM message_channel')

                    # Статистика по типам контента
                    stats = await conn.fetchrow('''
                        SELECT 
                            COUNT(CASE WHEN text_id IS NOT NULL THEN 1 END) as texts,
                            COUNT(CASE WHEN photo_id IS NOT NULL THEN 1 END) as photos,
                            COUNT(CASE WHEN video_id IS NOT NULL THEN 1 END) as videos,
                            COUNT(CASE WHEN audio_id IS NOT NULL THEN 1 END) as audio,
                            COUNT(CASE WHEN document_id IS NOT NULL THEN 1 END) as documents,
                            COUNT(CASE WHEN sticker_id IS NOT NULL THEN 1 END) as stickers,
                            COUNT(CASE WHEN animation_id IS NOT NULL THEN 1 END) as animations,
                            COUNT(CASE WHEN link_id IS NOT NULL THEN 1 END) as links
                        FROM message_channel
                    ''')

                    # Статистика ссылок
                    links_count = await conn.fetchval('SELECT COUNT(*) FROM links')
                    unique_domains = await conn.fetchval('SELECT COUNT(DISTINCT domain) FROM links')

                    # Последние 5 сообщений
                    recent_messages = await conn.fetch('''
                        SELECT mc.message_id, c.tag_channel, c.name_channel, mc.creation_time, mc.telegram_message_id
                        FROM message_channel mc
                        JOIN channel c ON mc.channel_id = c.channel_id
                        ORDER BY mc.creation_time DESC
                        LIMIT 5
                    ''')

                    text = f"""📊 <b>Статистика базы данных</b>

📺 <b>Каналы:</b>
• Всего: {channels_count}
• Активных: {active_channels}

💬 <b>Сообщения:</b>
• Всего: {messages_count}

📁 <b>Контент по типам:</b>
• Тексты: {stats['texts']}
• Фото: {stats['photos']}
• Видео: {stats['videos']}
• Аудио: {stats['audio']}
• Документы: {stats['documents']}
• Стикеры: {stats['stickers']}
• Анимации: {stats['animations']}
• Ссылки: {stats['links']}

🔗 <b>Ссылки:</b>
• Всего уникальных: {links_count}
• Уникальных доменов: {unique_domains}

🕒 <b>Последние сообщения:</b>"""

                    for msg in recent_messages:
                        time_str = msg['creation_time'].strftime(
                            '%d.%m.%Y %H:%M') if msg['creation_time'] else 'неизвестно'
                        text += f"\n• {msg['tag_channel']} ({msg['name_channel']}) - {time_str}"

                    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=self._main_keyboard())
            except Exception as e:
                logger.error(f"Error getting stats: {e}")
                await message.answer(f"❌ Ошибка получения статистики: {e}")

        @self.router.message(Command("collect"))
        async def cmd_collect(message: Message, command: CommandObject):
            if not command.args:
                await message.answer("❌ Укажите канал. Пример: /collect @channelname")
                return
            raw = command.args.strip()
            # allow accidental leading '@' before a link
            if raw.startswith('@http://') or raw.startswith('@https://'):
                raw = raw[1:]

            # Normalize input: support @username, t.me/username[/...], t.me/+invite, t.me/joinchat/invite
            channel = raw
            if channel.startswith('https://t.me/') or channel.startswith('http://t.me/'):
                tail = channel.split('t.me/', 1)[1]
                # invite links
                if tail.startswith('+'):
                    invite_hash = tail[1:].split('/', 1)[0]
                    try:
                        await self.tg_client(ImportChatInviteRequest(invite_hash))
                        await message.answer("✅ Присоединился по инвайт-ссылке. Пробую добавить канал...")
                    except Exception as e:
                        await message.answer(f"❌ Не удалось присоединиться по инвайту: {e}")
                        return
                    # cannot infer username; user should send @username after join if needed
                    # attempt is to proceed later by entity resolution on full link fails, so ask user to resend
                    await message.answer("ℹ️ Отправьте повторно команду с @username этого канала")
                    return
                if tail.startswith('joinchat/'):
                    invite_hash = tail.split('/', 1)[1].split('/', 1)[0]
                    try:
                        await self.tg_client(ImportChatInviteRequest(invite_hash))
                        await message.answer("✅ Присоединился по инвайт-ссылке. Пробую добавить канал...")
                    except Exception as e:
                        await message.answer(f"❌ Не удалось присоединиться по инвайту: {e}")
                        return
                    await message.answer("ℹ️ Отправьте повторно команду с @username этого канала")
                    return
                if tail.startswith('c/'):
                    # private/internal ID links cannot be resolved без членства; попросим username
                    await message.answer("⚠️ Ссылка вида t.me/c/... не содержит username. Укажите @username публичного канала.")
                    return
                # public username path; strip possible /post
                uname = tail.split('/', 1)[0]
                channel = '@' + uname
            if not channel.startswith('@'):
                channel = '@' + channel

            if not self.tg_client:
                await message.answer("⚠️ Telethon не настроен или используется бот-сессия. Нужна пользовательская сессия (не бот). Укажите TELEGRAM_API_ID, TELEGRAM_API_HASH, TELETHON_SESSION в .env")
                return
            ok = await self._add_channel(channel, message.from_user.id if message.from_user else 0)
            if ok:
                logger.info(
                    f"Channel {channel} added to monitoring by user {message.from_user.id if message.from_user else 'unknown'}")
                await message.answer(f"✅ Канал {channel} добавлен в мониторинг", reply_markup=self._main_keyboard())
            else:
                await message.answer(
                    f"❌ Не удалось добавить канал {channel}.\n"
                    f"Причины: приватный канал/нет доступа, неверный username, ограничение по стране/возрасту.\n"
                    f"Совет: проверьте @username, или пришлите инвайт-ссылку t.me/+... для автоматического присоединения.", reply_markup=self._main_keyboard())

        @self.router.message(Command("stop"))
        async def cmd_stop(message: Message, command: CommandObject):
            if not command.args:
                await message.answer("❌ Укажите канал. Пример: /stop @channelname")
                return
            channel = command.args.strip()
            if channel.startswith('@http://') or channel.startswith('@https://'):
                channel = channel[1:]
            if channel.startswith('https://t.me/'):
                channel = channel.replace('https://t.me/', '')
            if not channel.startswith('@'):
                channel = '@' + channel
            if await self._stop_channel(channel):
                logger.info(
                    f"Channel {channel} monitoring stopped by user {message.from_user.id if message.from_user else 'unknown'}")
                await message.answer(f"✅ Сбор с {channel} остановлен", reply_markup=self._main_keyboard())
            else:
                await message.answer(f"❌ {channel} не найден в списке", reply_markup=self._main_keyboard())

        @self.router.message(Command("fetch"))
        async def cmd_fetch(message: Message, command: CommandObject):
            """Скачать последние 2 сообщения из канала по username (через Telethon)."""
            if not command.args:
                await message.answer("❌ Укажите канал. Пример: /fetch @channelname")
                return
            channel = command.args.strip()
            if channel.startswith('https://t.me/'):
                channel = channel.replace('https://t.me/', '')
            if not channel.startswith('@'):
                channel = '@' + channel

            if not self.tg_client:
                await message.answer(
                    "⚠️ Не настроен доступ через Telethon. Укажите TELEGRAM_API_ID, TELEGRAM_API_HASH и TELETHON_SESSION в .env"
                )
                return

            saved = await self.fetch_last_messages(channel, limit=2)
            logger.info(
                f"Fetched {saved} messages from {channel} by user {message.from_user.id if message.from_user else 'unknown'}")
            if saved > 0:
                await message.answer(f"✅ Сохранено сообщений: {saved}")
            else:
                await message.answer("⚠️ Не удалось сохранить сообщения (канал приватный или нет доступа)")

        @self.router.message(F.media_group_id)
        async def handle_media_group(message: Message):
            """Обработка медиа-групп"""
            await self._handle_media_group(message)

        @self.router.message(~F.media_group_id)
        async def handle_single_message(message: Message):
            """Обработка одиночных сообщений"""
            await self._process_message_content(message)

        @self.router.edited_message()
        async def handle_edited_message(edited_message: Message):
            """Обработка отредактированных сообщений"""
            logger.info(
                f"Processing edited message {edited_message.message_id}")
            await self._process_message_content(edited_message)

        @self.router.callback_query()
        async def handle_callback(query: CallbackQuery):
            try:
                data = query.data or ""
                if data.startswith("stop|"):
                    channel = data.split("|", 1)[1]
                    if await self._stop_channel(channel):
                        await query.answer("Остановлено")
                        await query.message.edit_reply_markup(reply_markup=self._list_inline_keyboard())
                    else:
                        await query.answer("Не найдено")
                else:
                    await query.answer()
            except Exception:
                await query.answer()

    async def start(self):
        """Запуск бота"""
        logger.info("Starting Content Collector Bot...")
        try:
            # Инициализация базы данных
            await self._init_database()

            # Загружаем активные каналы из базы данных
            if self.db_pool:
                async with self.db_pool.acquire() as conn:
                    rows = await conn.fetch('''
                        SELECT channel_id, tag_channel, is_active, last_message_id
                        FROM channel
                        WHERE is_active = TRUE
                    ''')
                    for row in rows:
                        self.monitored_channels[row['tag_channel']] = {
                            'is_active': row['is_active'],
                            'last_id': row['last_message_id'] or 0,
                            'channel_id': row['channel_id']
                        }

            # Инициализация Telethon (если заданы креды)
            await self._init_telethon()
            # Запуск polling
            if self.tg_client and not self.monitor_task:
                self.monitor_task = asyncio.create_task(self._monitor_loop())
            await self.dp.start_polling(self.bot)
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            raise

    async def stop(self):
        """Остановка бота"""
        logger.info("Stopping Content Collector Bot...")
        if self.monitor_task:
            self.monitor_task.cancel()
            with contextlib.suppress(Exception):
                await self.monitor_task
        if self.tg_client:
            await self.tg_client.disconnect()
        if self.db_pool:
            await self.db_pool.close()
        await self.bot.session.close()


async def main():
    """Основная функция запуска бота"""
    # Получение токена и параметров из переменных окружения
    from dotenv import load_dotenv
    load_dotenv()

    def _clean(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        v = value.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        return v.strip()

    TOKEN = _clean(os.getenv('TELEGRAM_BOT_TOKEN'))
    DOWNLOAD_PATH = _clean(os.getenv('DOWNLOAD_PATH')) or './downloads'
    DATABASE_URL = _clean(os.getenv(
        'DATABASE_URL')) or 'postgresql://palachick:20power@localhost:5432/content_collector_db'
    # Telethon (опционально, для чтения истории публичных каналов как пользователь)
    TELEGRAM_API_ID = _clean(os.getenv('TELEGRAM_API_ID'))
    TELEGRAM_API_HASH = _clean(os.getenv('TELEGRAM_API_HASH'))
    TELETHON_SESSION = _clean(os.getenv('TELETHON_SESSION'))
    NOTIFICATION_CHAT_ID = _clean(os.getenv('NOTIFICATION_CHAT_ID'))

    if not TOKEN:
        raise ValueError("Please set TELEGRAM_BOT_TOKEN environment variable")

    # Создание и запуск бота
    bot = ContentCollectorBot(
        TOKEN,
        DATABASE_URL,
        DOWNLOAD_PATH,
        int(TELEGRAM_API_ID) if TELEGRAM_API_ID else None,
        TELEGRAM_API_HASH,
        TELETHON_SESSION,
        int(NOTIFICATION_CHAT_ID) if NOTIFICATION_CHAT_ID else None
    )

    try:
        await bot.start()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    finally:
        await bot.stop()

if __name__ == "__main__":
    # Запуск бота
    asyncio.run(main())
