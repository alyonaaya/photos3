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

# Данные для авторизации
access_key = ""
secret_key = ""
storage_url = 'http://localhost:9000'


"""
Асинхронные функции.
Каждая выполняет одно завершённое действие.
Предназначены для добавления в цикл событий.
"""


async def upload_one_file(src, dest, bucket, loop):
    """
    Функция загружает один файл в хранилище
    :param src: Путь до файла в фаловой система
    :param dest: Путь до файла в бакете
    :param bucket: Название бакета
    :param loop:
    :return:
    """
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3",
                                     endpoint_url=storage_url,
                                     region_name='us-west-2',
                                     aws_secret_access_key=secret_key,
                                     aws_access_key_id=access_key) as client:
        resp = await client.put_object(Bucket=bucket, Key=dest, Body=open(src, "rb"))
        print("{} uploaded".format(src))


async def download_one_file(src, dest, bucket, loop):
    """
    Функция скачивает однин файл
    :param src: Путь до файла в бакете
    :param dest: Путь, куда будет сохранён файл
    :param bucket: Название бакета
    :param loop:
    :return:
    """
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3",
                                     endpoint_url=storage_url,
                                     region_name='us-west-2',
                                     aws_secret_access_key=secret_key,
                                     aws_access_key_id=access_key) as client:
        try:
            resp = await client.get_object(Bucket=bucket, Key=src)
        except ClientError as ce:
            sys.exit(ce.response["Error"]["Message"])
        async with resp["Body"] as stream:
            data = await stream.read()
            with open(dest, "wb") as out_file:
                out_file.write(data)
            print("{} downloaded to {}".format(src, dest))


async def list_bucket(bucket, loop):
    """
    Функция для просмотра файлов в бакете
    :param bucket: Название бакета
    :param loop:
    :return:
    """
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3",
                                     endpoint_url=storage_url,
                                     region_name='us-west-2',
                                     aws_secret_access_key=secret_key,
                                     aws_access_key_id=access_key) as client:

        try:
            resp = await client.list_objects(Bucket=bucket)
        except ClientError as ce:
            sys.exit(ce.response["Error"]["Message"])
        for obj in resp["Contents"]:
            owner_name = obj["Owner"]["DisplayName"]
            owner_name = "unknown" if len(owner_name) == 0 else owner_name
            owner_id = obj["Owner"]["ID"]
            dt = obj["LastModified"]
            key = obj["Key"]
            print("{} {} {} {}".format(owner_name, owner_id, dt.strftime("%Y-%m-%d %H:%M:%S %Z"), key))


async def download_bucket(bucket, loop):
    """
    Функция для скачивания всего бакета в текущую директорию.
    Для параллельного скачивания файлов запускает внутренний асинхронный цикл
    :param bucket: Название бакета
    :param loop:
    :return:
    """
    download_tasks = list()
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3",
                                     endpoint_url=storage_url,
                                     region_name='us-west-2',
                                     aws_secret_access_key=secret_key,
                                     aws_access_key_id=access_key) as client:

        # Сначала формируется список файлов, содержащхся в бакете
        try:
            resp = await client.list_objects(Bucket=bucket)
        except ClientError as ce:
            sys.exit(ce.response["Error"]["Message"])
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
            out_file_name = os.path.join(outdir, os.path.basename(key))
            # На основе списка файлов из хранилища формируются задания на скачивание файлов по одному
            download_tasks.append(asyncio.ensure_future(download_one_file(key, out_file_name, bucket, loop)))
        # Внутренний асинхронный цикл
        await asyncio.wait(download_tasks)


"""
Функции, формирующие задания.
Высчитывают параметры для каждого задания, формируют очереди заданий.
"""


def upload(src, bucket, tasks, loop):
    """
    Функция получает список всех файлов из всех подпапок.
    Формирует задания на загрузку файлов в хранилице.
    :param src: Путь до директории, которая должна быть загружена в хрнаилище
    :param bucket: Бакет, в который будет помещен каталог
    :param tasks: Список заданий для асинхронного цикла
    :param loop: Сам цикл
    :return:
    """
    if not os.path.isdir(src):
        print("{} is not a directory".format(src))

    src_full = os.path.abspath(src)
    one_level_upper = os.path.abspath(os.path.join(src_full, "../"))

    for path, dirs, files in os.walk(src_full):
        key = path[len(one_level_upper) + 1:]
        for file in files:
            in_file_name = os.path.join(path, file)
            out_file_name = os.path.join(key, file)
            tasks.append(asyncio.ensure_future(upload_one_file(in_file_name, out_file_name, bucket, loop)))


def download(bucket, tasks, loop):
    """
    Функция для скачивания бакета.
    Формирует одно задание для цикла.
    Распараллеливание скачивания осуществляется внутри этого задания.
    :param bucket: Название бакета.
    :param tasks:
    :param loop:
    :return:
    """
    tasks.append(asyncio.ensure_future(download_bucket(bucket, loop)))


def get(src, dest, bucket, tasks, loop):
    """
    Функция формирует задание на скачивание одного файла.
    :param src: Путь до файла в бакете
    :param dest: Директория, куда сохранить файл
    :param bucket: Название бакета
    :param tasks:
    :param loop:
    :return:
    """
    if not os.path.isdir(dest):
        sys.exit("{} is not directory".format(dest))

    out_file_name = os.path.join(dest, os.path.basename(src))
    tasks.append(asyncio.ensure_future(download_one_file(src, out_file_name, bucket, loop)))


def lst(bucket, tasks, loop):
    """
    Функция формирует задание на получение списка файлов в бакете
    :param bucket: Назание бакета
    :param tasks:
    :param loop:
    :return:
    """
    tasks.append(asyncio.ensure_future(list_bucket(bucket, loop)))


@click.command()
@click.option("--login", "-l", default="", help="Access key")
@click.option("--password", "-p", default="", help="Secret key")
@click.option("--command", "-c", default="list", help="commands: {}".format(", ".join(map(str, commands))))
@click.option("--src", "-s", default=".", help="/path/to/source")
@click.option("--dest", "-d", default=".", help="/path/to/dest")
@click.option("--bucket", "-b", default="mybucket", help="bucket name")
def main(login, password, command, src, dest, bucket):
    """CLI interface for Amazon S3"""

    # Часто используемые переменные в разных функциях
    global access_key
    global secret_key

    access_key = login
    secret_key = password

    # Список заданий для асинхронного цикла
    tasks = list()
    loop = asyncio.get_event_loop()

    # На основе введённоё команды формируется список заданий
    if command == C_DOWNLOAD:
        download(bucket, tasks, loop)
    elif command == C_GET:
        get(src, dest, bucket, tasks, loop)
    elif command == C_LIST:
        lst(bucket, tasks, loop)
    elif command == C_UPLOAD:
        upload(src, bucket, tasks, loop)
    else:
        sys.exit("Unknown command {}".format(command))
    # Сформированный список заданий подаётся в цикл
    loop.run_until_complete(asyncio.wait(tasks))


if __name__ == "__main__":
    main()

