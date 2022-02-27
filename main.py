# !/usr/bin/env python3
import os
import asyncio
import subprocess
from ruamel import yaml
from loguru import logger as log
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterPhotos
from telethon.tl.types import InputMessagesFilterVideo
from telethon.tl.types import InputMessagesFilterDocument
from telethon.tl.types import InputMessagesFilterMusic
from telethon.tl.types import InputMessagesFilterVoice

session_name = 'hello'
api_id = 14150995
api_hash = os.environ.get('tg_api_hash')
dir_prefix = './downloads/'
config_file = './config.yaml'
proxy = ("socks5", '127.0.0.1', 10808)
client = TelegramClient(session_name, api_id, api_hash, proxy=None)
max_worker_num = 8


async def get_chat_id_by_name(_client, _name):
    async for dialog in _client.iter_dialogs():
        if _name in dialog.name:
            return dialog.name, dialog.entity.id


async def fetch_message(down_queue, v_chat, v_offset, v_filter):
    async for message in client.iter_messages(v_chat, reverse=True, offset_id=v_offset, limit=None, filter=v_filter):
        filename = message.file.name
        if filename is None:
            filename = f'{message.id}{message.file.ext}'
        message.file_type = v_filter.__name__[19:]
        message.file_name = f'[({message.file_type})({message.id}){message.message}]{filename}'
        await down_queue.put(message)
        log.info('fetch message {}', message.file_name)
    for i in range(max_worker_num):
        await down_queue.put(None)


async def download_worker(down_queue, up_queue):
    while True:
        message = await down_queue.get()
        try:
            if message is None:
                break
            st = os.statvfs('./')
            if st.f_bavail * st.f_frsize / 1024 / 1024 / 1024 < 2:
                log.error('the available disk capacity is lower than 2GB, stop to download!')
                break
            dirname = dir_prefix + f'({message.chat.title}{message.chat.id})/' + message.date.strftime("%Y-%m")
            message.file_path = os.path.join(dirname, message.file_name)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            if os.path.exists(message.file_path):
                if os.path.getsize(message.file_path) < message.file.size:
                    os.remove(message.file_path)
                else:
                    log.info('already exist file {}', message.file_name)
                    await up_queue.put(message)
                    continue
            await client.download_media(message, message.file_path)
            await up_queue.put(message)
            log.info('successfully download {}', message.file_name)
        except Exception as e:
            log.error('failed to download {}: {}', message.file_name, e)
        finally:
            down_queue.task_done()
    await up_queue.put(None)


async def upload_worker(up_queue):
    while True:
        message = await up_queue.get()
        try:
            if message is None:
                break
            remote = 'root@chaosi-zju.com:/root/tg_downloader/downloads'
            command = ['rsync', '-avzP', '--rsh=ssh', message.file_path, remote]
            if subprocess.call(command) != 0:
                raise Exception('failed to call command: {}'.format(command))
            config[message.chat.id][message.file_type] = message.id
            log.info('successfully rsync {}', message.file_path)
        except Exception as e:
            log.error('failed to rsync {}: {}', message.file_path, e)
        finally:
            up_queue.task_done()


def get_offset_from_config(chat, _type):
    typename = _type.__name__[19:]
    if chat not in config:
        config[chat] = {}
    if typename not in config[chat]:
        config[chat][typename] = 1
    return config[chat][typename]


def save_config_to_file():
    with open(config_file, 'w') as wf:
        yaml.dump(config, wf, Dumper=yaml.RoundTripDumper)


async def main():
    name, chat = await get_chat_id_by_name(client, '全球')

    _types = [InputMessagesFilterPhotos, InputMessagesFilterMusic, InputMessagesFilterVoice,
              InputMessagesFilterVideo, InputMessagesFilterDocument]

    for _type in _types:
        offset = get_offset_from_config(chat, _type)
        log.info('sub process for chat={} _type={} offset={} started!', chat, _type.__name__[19:], offset)

        down_queue = asyncio.Queue(max_worker_num)
        up_queue = asyncio.Queue(max_worker_num)

        prod = asyncio.create_task(fetch_message(down_queue, chat, offset, _type))

        for i in range(max_worker_num):
            asyncio.create_task(download_worker(down_queue, up_queue))

        tasks = []
        for i in range(max_worker_num):
            tasks.append(asyncio.create_task(upload_worker(up_queue)))

        for task in tasks:
            await task

        save_config_to_file()
        if prod.done() is False:
            prod.cancel()
            log.error('producer has not done, stop for possible problems!')
            break


if __name__ == '__main__':
    log.add("./logs/{time:YYYY-MM-DD__HH}.log", rotation="1h")
    log.add("./logs/{time:YYYY-MM-DD}.error.log", level="ERROR", rotation="1 days")
    client.start()
    loop = asyncio.get_event_loop()
    try:
        with open(config_file, 'a+') as f:
            f.seek(0)
            config = yaml.load(f, Loader=yaml.RoundTripLoader)
            config = {} if config is None else config
        log.info('----start to download!----')
        loop.run_until_complete(main())
    finally:
        save_config_to_file()
        client.disconnect()
        loop.close()
        log.info('----download finished, gracefully stopped!----')
