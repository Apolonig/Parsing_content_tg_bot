from telethon import TelegramClient
from telethon.sessions import StringSession
api_id = int(input("API_ID: "))
api_hash = input("API_HASH: ")
with TelegramClient(StringSession(), api_id, api_hash) as client:
    print(client.session.save())