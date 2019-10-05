#!/usr/bin/env python3

import asyncio
import aiobotocore
import click
import os
import sys
from botocore.exceptions import ClientError


'''
Примеры использования
    Скачать файл:
        photo.py -l <access_key> -p <secret_key>  -c get -b <bucket_name> -s path/in/bucket.JPG -d /save/folder/
    Загрузить директорию в хранилище:
        photo.py -l <access_key> -p <secret_key>  -c upload -s /path/to/folder -b <bucket_name>
    Скачать бакет в текущую диреткорию:
        photo.py -l <access_key> -p <secret_key>  -c download -b <bucket_name>
    Посмотреть список файлов в бакете:
        photo.py -l <access_key> -p <secret_key>  -c list -b <bucket_name>
'''

# Список команд
C_DOWNLOAD = "download"
C_GET = "get"
C_LIST = "list"
C_UPLOAD = "upload"
commands = [C_DOWNLOAD, C_GET, C_LIST, C_UPLOAD]


"""
Конфигурация
"""
# Адрес хранилища
storage_url = 'http://localhost:9000'
# Число одновременных запросов и лимит на число одновременно запущенных корутин
concurrency = 10


def error_handler(f):
    """
    Обработчик ошибок
    :param f: Асинхронная функция
    :return: Пытается выполнить, иначе обработать возникшую ошибку
    """
    async def wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except ClientError as ce:
            sys.exit(ce.response["Error"]["Message"])
        except FileNotFoundError as fne:
            sys.exit(str(fne))
        except ValueError as ve:
            sys.exit(str(ve))
    return wrapper


"""
Асинхронные функции.
Каждая выполняет одно завершённое действие.
Предназначены для добавления в цикл событий.
"""


async def upload_one_file(src, dest, bucket, client, sem=asyncio.Semaphore(1)):
    """
    Функция загружает один файл в хранилище
    :param src: Путь до файла в фаловой система
    :param dest: Путь до файла в бакете
    :param bucket: Название бакета
    :param client:
    :param sem:
    :return:
    """
    async with sem:
        resp = await client.put_object(Bucket=bucket, Key=dest, Body=open(src, "rb"))
        print("{} uploaded".format(src))


async def download_one_file(src, dest, bucket, client, sem=asyncio.Semaphore(1)):
    """
    Функция скачивает однин файл
    :param src: Путь до файла в бакете
    :param dest: Путь, куда будет сохранён файл
    :param bucket: Название бакета
    :param client:
    :param sem
    :return:
    """
    async with sem:
        resp = await client.get_object(Bucket=bucket, Key=src)
        async with resp["Body"] as stream:
            out_file_name = os.path.join(dest, os.path.basename(src))
            with open(out_file_name, "wb") as out_file:
                while True:
                    data = await stream.read(1024)
                    if len(data) == 0:
                        break
                    out_file.write(data)
            print("{} downloaded to {}".format(src, out_file_name))


async def list_bucket(bucket, client):
    """
    Функция для просмотра файлов в бакете
    :param bucket: Название бакета
    :param client:
    :return:
    """
    resp = await client.list_objects(Bucket=bucket)
    for obj in resp["Contents"]:
        owner_name = obj["Owner"]["DisplayName"]
        owner_name = "unknown" if len(owner_name) == 0 else owner_name
        owner_id = obj["Owner"]["ID"]
        dt = obj["LastModified"]
        key = obj["Key"]
        print("{} {} {} {}".format(owner_name, owner_id, dt.strftime("%Y-%m-%d %H:%M:%S %Z"), key))


async def download_bucket(bucket, client):
    """
    Функция для скачивания всего бакета в текущую директорию.
    Для параллельного скачивания файлов запускает внутренний асинхронный цикл
    :param bucket: Название бакета
    :param client:
    :return:
    """
    download_tasks = list()

    # Семафор ограничивает потребляемые ресурсы
    sem = asyncio.Semaphore(concurrency)

    # Сначала формируется список файлов, содержащхся в бакете
    resp = await client.list_objects(Bucket=bucket)
    for obj in resp["Contents"]:
        directories = [bucket]
        outdir = ""
        key = obj["Key"]
        path = key.split("/")
        directories.extend(path[:-1])
        for directory in directories:
            outdir = os.path.join(outdir, directory)
            if not os.path.exists(outdir):
                os.mkdir(outdir)
        # На основе списка файлов из хранилища формируются задания на скачивание файлов по одному
        download_tasks.append(asyncio.ensure_future(download_one_file(key, outdir, bucket, client, sem)))
        # Внутренний асинхронный цикл
    await asyncio.wait(download_tasks)


"""
Функции, формирующие задания.
Высчитывают параметры для каждого задания, формируют очереди заданий.
"""


async def upload(src, bucket, client):
    """
    Функция получает список всех файлов из всех подпапок.
    Формирует задания на загрузку файлов в хранилице.
    :param src: Путь до директории, которая должна быть загружена в хрнаилище
    :param bucket: Бакет, в который будет помещен каталог
    :param client:
    :return:
    """
    # Семафор ограничивает, потребляемые корутинами ресурсы
    sem = asyncio.Semaphore(concurrency)
    upload_tasks = list()

    src_full = os.path.abspath(src)
    one_level_upper = os.path.abspath(os.path.join(src_full, "../"))

    conum = 0

    for path, dirs, files in os.walk(src_full):
        key = path[len(one_level_upper) + 1:]
        for file in files:
            conum += 1
            in_file_name = os.path.join(path, file)
            out_file_name = os.path.join(key, file)
            upload_tasks.append(asyncio.ensure_future(upload_one_file(in_file_name, out_file_name, bucket, client, sem)))

            '''# Как только достигаем лимита на запущенные корутины, запускаем их
            if conum == limit:
                await asyncio.wait(upload_tasks)
                print("Uploaded {} files".format(len(upload_tasks)))
                upload_tasks.clear()
                conum = 0

    # Выполняем оставшееся
    if len(upload_tasks) != 0:
        print("Uploaded {} files".format(len(upload_tasks)))
        await asyncio.wait(upload_tasks)'''
    await asyncio.wait(upload_tasks)


@error_handler
async def async_main(login, password, command, src, dest, bucket, loop):
    """
    Асинхронная функция, обрабатывающая команды
    :param login: Логин
    :param password: Пароль
    :param command: Команда
    :param src: /path/to/src
    :param dest: /path/to/dest
    :param bucket: Название бакета
    :param loop: Асинхронный цикл
    :return:
    """

    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3",
                                     endpoint_url=storage_url,
                                     region_name='us-west-2',
                                     aws_secret_access_key=password,
                                     aws_access_key_id=login) as client:

        if command == C_DOWNLOAD:
            await download_bucket(bucket, client)
        elif command == C_GET:
            await download_one_file(src, dest, bucket, client)
        elif command == C_LIST:
            await list_bucket(bucket, client)
        elif command == C_UPLOAD:
            await upload(src, bucket, client)
        else:
            sys.exit("Unknown command {}".format(command))


@click.command()
@click.option("--login", "-l", default="", help="Access key")
@click.option("--password", "-p", default="", help="Secret key")
@click.option("--command", "-c", default="list", help="commands: {}".format(", ".join(map(str, commands))))
@click.option("--src", "-s", default=".", help="/path/to/source")
@click.option("--dest", "-d", default=".", help="/path/to/dest")
@click.option("--bucket", "-b", default="mybucket", help="bucket name")
def main(login, password, command, src, dest, bucket):
    """CLI interface for Amazon S3"""

    # Сформированный список заданий подаётся в цикл
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_main(login, password, command, src, dest, bucket, loop))


if __name__ == "__main__":
    main()

