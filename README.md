# TG_Downloader

#### 运行方法

先在 `tg_downloader.service` 中配置tg的api_hash值及文件压缩密码

```bash
systemctl stop tg_downloader.service
systemctl disable tg_downloader.service
cp -rf ./tg_downloader.service /etc/systemd/system/
systemctl enable tg_downloader.service
systemctl start tg_downloader.service
systemctl status tg_downloader.service
journalctl --since "2022-03-11 23:00:00" -u tg_downloader.service -f
```