# !/usr/bin/env python3
import os
import re
import sqlite3
import asyncio
import subprocess
from loguru import logger as log
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterPhotos
from telethon.tl.types import InputMessagesFilterVideo
from telethon.tl.types import InputMessagesFilterDocument
from telethon.tl.types import InputMessagesFilterMusic
from telethon.tl.types import InputMessagesFilterVoice

# tg_client
chat_name, chat_id = 'charles', 0
session_name = 'hello'
api_id = 14150995
api_hash = os.environ.get('tg_api_hash')
proxy = ("socks5", '127.0.0.1', 10808)
client = TelegramClient(session_name, api_id, api_hash, proxy=None)
# sqlite3
conn = sqlite3.connect('./downloader.db')
cur = conn.cursor()
# conf
dir_prefix = './downloads/'
yunpan_path = '/root/Program/aliyunpan/'
onedrive_path = '/root/OneDrive/tg_download/'
max_worker_num = alive_worker_num = 8
batch_upload_num = 3
enable_upload = False
zip_passwd = os.environ.get('zip_passwd')


def init_sqlite_db():
    try:
        cur.execute('CREATE TABLE IF NOT EXISTS task(chatname TEXT, batchname TEXT, uploaded INT, files TEXT)')
        conn.commit()
    except Exception as e:
        log.error('create sqlite table failed: {}', e)


async def get_chat_id_by_name(_client, _name):
    async for dialog in _client.iter_dialogs():
        if _name in dialog.name:
            return dialog.name, dialog.entity.id


async def fetch_message(v_chat, v_offset, v_filter):
    messages = []
    async for message in client.iter_messages(v_chat, reverse=True, offset_id=v_offset, limit=None, filter=v_filter):
        if message.media is not None:
            filename = message.file.name
            if filename is None:
                filename = f'{message.id}{message.file.ext}'
            message.file_type = v_filter.__name__[19:]
            # message_text = ''.join(re.findall(re.compile(u'[\u4e00-\u9fa5]'), message.message))[:15]
            message.file_name = f'【{message.file_type}-{message.id}】{filename}'
            messages.append(message)
    return messages


async def put_messages_to_queue(chat, _type, messages, down_queue):
    batch, batch_size, batch_num = [], 0, 0
    typename, min_id, max_id, size = '', 1, 1, 0
    for msg in messages:
        min_id = msg.id if msg.id < min_id or min_id == 1 else min_id
        max_id = msg.id if msg.id > max_id else max_id
        size += msg.file.size
        if batch_size + msg.file.size < 1 * 1024 * 1024 * 1024:  # less than 1G for one batch
            batch.append(msg)
            batch_size += msg.file.size
        else:
            if len(batch) > 0:
                await down_queue.put((batch_num, batch))
                batch_num += 1
            batch_size = msg.file.size
            batch = [msg]
    if len(batch) > 0:
        await down_queue.put((batch_num, batch))
        batch_num += 1
    log.info('chat({}) has {} {}, min_id: {}, max_id: {}, size: {}MB, batch_num: {}', chat, len(messages),
             _type.__name__[19:], min_id, max_id, int(size / 1024 / 1024), batch_num)


async def download_worker(down_queue):
    while down_queue.empty() is False:
        batch_id, batch = await down_queue.get()
        try:
            st = os.statvfs('./')
            if st.f_bavail * st.f_frsize / 1024 / 1024 / 1024 < 5:
                log.error('the available disk capacity is lower than 5GB, stop to download!')
                break

            chatname = f'{chat_name}{chat_id}'
            batchdir = os.path.join(dir_prefix, chatname, f'batch-{batch_id}')
            batchname = f'{chat_name}{chat_id}-{batch[0].file_type}-batch-{batch_id}.zip'

            cur.execute(f"select * from task where chatname = '{chatname}' and batchname = '{batchname}'")
            if cur.fetchone() is not None:
                log.info('already downloaded batch {}', batchname)
                continue

            if not os.path.exists(onedrive_path + batchname):
                if not os.path.exists(batchdir):
                    os.makedirs(batchdir)
                for msg in batch:
                    msg.file_path = os.path.join(batchdir, msg.file_name)
                    if os.path.exists(msg.file_path):
                        if os.path.getsize(msg.file_path) < msg.file.size:
                            os.remove(msg.file_path)
                        else:
                            log.info('already exist file {}', msg.file_name)
                            continue
                    # open(msg.file_path, 'w')
                    # await asyncio.sleep(1)
                    await client.download_media(msg, msg.file_path)
                    log.info('successfully download {}', msg.file_name)

                command = ['zip', '-mrP', zip_passwd, onedrive_path + batchname, batchdir]
                if subprocess.call(command) != 0:
                    raise Exception('failed to call command: {}'.format(command))

            files = '、'.join([msg.file_name for msg in batch])
            cur.execute(f"insert into task values ('{chatname}', '{batchname}', 0, '{files}')")
            conn.commit()
            log.info('successfully zipped batch {}', batchname)
        except Exception as e:
            log.error('failed to download {}: {}', batch[0].file_name, e)
        finally:
            down_queue.task_done()
    global alive_worker_num
    alive_worker_num -= 1


# Deprecated
async def upload_worker():
    retry, fail, batchs = 0, 0, ''
    while True:
        try:
            cur.execute(f'select batchname from task where uploaded = 0 limit {batch_upload_num}')
            data = cur.fetchall()
            if len(data) == 0:
                if alive_worker_num == 0:
                    if retry >= 1:
                        break
                    retry += 1
                await asyncio.sleep(60)
                continue

            # await asyncio.sleep(2)
            paths = [onedrive_path + item[0] for item in data]
            command = [yunpan_path + 'aliyunpan', 'u'] + paths + ['tg_downloader']
            if subprocess.call(command) != 0:
                raise Exception('failed to call command: {}'.format(command))
            command = ['rm'] + paths
            if subprocess.call(command) != 0:
                raise Exception('failed to call command: {}'.format(command))

            batchs = "', '".join(item[0] for item in data)
            cur.execute(f"update task set uploaded = 1 where batchname in ('{batchs}')")
            conn.commit()
            retry, fail = 0, 0
            log.info('successfully uploaded {}', f"'{batchs}'")
        except Exception as e:
            log.error('failed to upload {}: {}', f"'{batchs}'", e)
            if fail >= 2:
                break
            fail += 1


async def main():
    init_sqlite_db()

    down_queue = asyncio.Queue()
    global chat_name, chat_id
    chat_name, chat_id = await get_chat_id_by_name(client, chat_name)
    msg_types = [InputMessagesFilterPhotos, InputMessagesFilterMusic, InputMessagesFilterVoice,
                 InputMessagesFilterVideo, InputMessagesFilterDocument]

    for _type in msg_types:
        messages = await fetch_message(v_chat=chat_id, v_offset=1, v_filter=_type)
        await put_messages_to_queue(chat_id, _type, messages, down_queue)

    tasks = []
    for i in range(max_worker_num):
        tasks.append(asyncio.create_task(download_worker(down_queue)))

    if enable_upload:
        tasks.append(asyncio.create_task(upload_worker()))

    for task in tasks:
        await task


if __name__ == '__main__':
    log.add("./logs/{time:YYYY-MM-DD__HH}.log", rotation="1h", retention="2 days")
    log.add("./logs/{time:YYYY-MM-DD}.error.log", level="ERROR", rotation="1 days", retention="2 days")
    client.start()
    loop = asyncio.get_event_loop()
    try:
        log.info('----application started!----')
        loop.run_until_complete(main())
    finally:
        client.disconnect()
        cur.close()
        conn.close()
        loop.close()
        log.info('----application ended, gracefully exit!----')
