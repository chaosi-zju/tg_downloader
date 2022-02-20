# !/usr/bin/env python3
import os
import asyncio
from telethon.sync import TelegramClient, errors
from telethon.tl.types import InputMessagesFilterPhotos
from telethon.tl.types import InputMessagesFilterVideo
from telethon.tl.types import InputMessagesFilterDocument
from telethon.tl.types import InputMessagesFilterMusic
from telethon.tl.types import InputMessagesFilterVoice

session_name = 'hello'
api_id = 14150995
api_hash = ''
max_worker_num = 8
dir_prefix = './downloads/'
delete_exist = True
proxy = ("socks5", '127.0.0.1', 10808)
client = TelegramClient(session_name, api_id, api_hash, proxy=proxy)


async def get_chat_id_by_name(_client, _name):
    async for dialog in _client.iter_dialogs():
        if _name in dialog.name:
            return dialog.name, dialog.entity.id


async def fetch_message(q, v_chat, v_filter):
    async for message in client.iter_messages(v_chat, reverse=True, offset_id=1, limit=1, filter=v_filter):
        filename = message.file.name
        if filename is None:
            filename = f'{message.id}{message.file.ext}'
        message.file_type = v_filter.__name__[19:]
        message.file_name = f'[({message.file_type})({message.id}){message.message}]{filename}'
        print('fetch message', message.file_name)
        await q.put(message)


async def download_worker(q, i):
    while True:
        message = await q.get()
        dirname = dir_prefix + f'({message.chat.title}{message.chat.id})/' + message.date.strftime("%Y-%m")
        filepath = os.path.join(dirname, message.file_name)
        # print('worker', i, 'start to download', message.file_name)
        try:
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            if os.path.exists(filepath):
                if os.path.getsize(filepath) < message.file.size:
                    os.remove(filepath)
                else:
                    print('already exist file', message.file_name)
                    continue
            # await asyncio.sleep(1)
            await client.download_media(message, filepath)
            print('successfully download', message.file_name)
        except errors.rpc_errors_re.FileReferenceExpiredError:
            async for msg in client.iter_messages(message.chat, offset_id=message.id - 1, reverse=True, limit=1):
                await q.put(msg)
        except Exception as e:
            print(f'{e}')
        finally:
            q.task_done()


async def main():
    q = asyncio.Queue()
    name, chat = await get_chat_id_by_name(client, '全球新鲜事')

    _types = [InputMessagesFilterVideo, InputMessagesFilterPhotos, InputMessagesFilterDocument,
              InputMessagesFilterMusic, InputMessagesFilterVoice]
    producers = [
        fetch_message(q, chat, v_filter=_type)
        for _type in _types
    ]

    consumers = [
        asyncio.ensure_future(download_worker(q, i))
        for i in range(max_worker_num)
    ]

    await asyncio.gather(*producers)
    await q.join()
    for consumer in consumers:
        consumer.cancel()


if __name__ == '__main__':
    client.start()
    loop = asyncio.get_event_loop()
    try:
        print('----start!----')
        loop.run_until_complete(main())
    finally:
        client.disconnect()
        loop.close()
        print('----download finished, gracefully stopped!----')
