"""
Оркестратор всего двух классов:
1. Формируем отчет в файл datasets_info.txt, который содержит список всех ЦЕЛЕВЫХ датасетов с перечислением их полей, которые лягут в основу таблицы в БД
2. Импортирует каждый датасет в свою таблицу в pg
"""

import os
from parser import MosTransportParser
from importer import SimpleImporter


def main():
    # Получаем API-ключ из переменной окружения MOS_API_KEY
    api_key = os.getenv("MOS_API_KEY")

    # 1) Формируем отчет о целевых датасетах
    #   a] Составляем список всех ID из категории 102 (Дороги и транспорт)
    #   b] Отфильтровываем по ключевым словам (метро/мцд/мцк/диаметр/кольц)
    #   c] Записываем их id, имя и поля в файл блокнота
    parser = MosTransportParser(api_key, outfile="datasets_info.txt")
    ids = parser.get_dataset_ids()  # Получаю список всех id в категории 102 (захардкодил, конечно)
    parser.dump_filtered(ids)  # Фильтр по упоминаниям метро и записываем отчет в текстовый фацл

    # 2) Импортируем каждый датасет в отдельную таблицу PG
    #    a] Читаем строку подключения
    #    b] Запускаем run, который:
    #        1] Для каждого целевого ID создаёт таблицу по метаданным (dataset_{id}_{slug})
    #        2] Загружает все записи через /v1/features/{id} и вставляет их батчем

    # Строка подключения к PostGIS
    pg_dsn = os.getenv(
        "PG_DSN",
        "dbname=mos_transport "
        "user=mos "
        "password=mos "
        "host=localhost "
        "port=5432"
    )
    importer = SimpleImporter(api_key,
                              pg_dsn)
    importer.run()


if __name__ == "__main__":
    main()
