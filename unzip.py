import os
import json
import subprocess
from loguru import logger as log

rar = 'C:\\Program Files\\WinRAR\\WinRAR.exe'
path_from = 'D:\\Download\\OneDrive - zju.edu.cn\\TGTP\\'
path_to = 'F:\\tg-download\\tp\\'

log.add("./logs/{time:YYYY-MM-DD__HH}.log", rotation="1 days")
log.add("./logs/{time:YYYY-MM-DD}.error.log", level="ERROR", rotation="1 days")

if not os.path.exists(path_from):
    log.error('path not exist!')
    exit(0)

try:
    with open('./unzip.json', 'r', encoding='utf-8') as fs:
        okfiles = json.load(fs)
except (IOError, Exception) as e:
    open('./unzip.json', 'w')
    okfiles = []

files = os.listdir(path_from)
for file in files:
    if file in okfiles:
        log.info(f'already unzipped {file}')
        continue
    command = [rar, 'e', path_from + file, path_to, '-ppcs', '-o-', '-ibck']
    if subprocess.call(command) != 0:
        log.error(f'failed to call command: {command}')
    else:
        log.info(f'successful unzip {file}')
        okfiles.append(file)

with open('./unzip.json', 'w', encoding='utf-8') as fs:
    json.dump(okfiles, fs, indent=4, ensure_ascii=False)

# prefix = '墙眼 -'
# subfiles = os.listdir(path_to)
# for file in subfiles:
#     if '.' not in file:
#         continue
#     if prefix not in file:
#         os.rename(path_to + file, path_to + prefix + file)
#         log.info(f'renamed {file}')
