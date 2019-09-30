# photos3

Консольная утилита для работы с S3.

## Примеры использования

Скачать файл:

    photo.py -l <access_key> -p <secret_key>  -c get -b <bucket_name> -s path/in/bucket.JPG -d /save/folder/

Загрузить директорию в хранилище:

    photo.py -l <access_key> -p <secret_key>  -c upload -s /path/to/folder -b <bucket_name>

Скачать бакет в текущую диреткорию:

    photo.py -l <access_key> -p <secret_key>  -c download -b <bucket_name>

Посмотреть список файлов в бакете:

    photo.py -l <access_key> -p <secret_key>  -c list -b <bucket_name>

Адрес хранилища захардкожен, что бы слишком не перегружать интерфейс: http://localhost:9000 