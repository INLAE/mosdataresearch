"""
Транспортирую каждый датасет:
 - Для каждого датасета создаётся таблица dataset_{id}_{slug} по метаданным,
 - Затем все фичи из /v1/features/{id} вставляются батчем.
"""

import re
import json
import requests
import psycopg2
from shapely.geometry import Point
from psycopg2.extras import execute_values

from parser import MosTransportParser


def slugify(name: str) -> str:
    # Преобразует строку в приемлемое имя для таблицы
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_{2,}", "_", s).strip("_") or "dataset"


class SimpleImporter:
    def __init__(self, api_key: str, pg_dsn: str):

        self.parser = MosTransportParser(api_key)
        self.http = requests.Session()
        self.http.params = {"api_key": api_key}  # Вставляем API-ключ в каждый запрос
        self.pg = psycopg2.connect(pg_dsn)
        self.cur = self.pg.cursor()

        # fix: маппинг типов данных из датасета в типы данных pg
        self.TYPE_MAP = {
            "STRING": "TEXT",
            "INTEGER": "INTEGER",
            "NUMBER": "NUMERIC",
            "DICTIONARY": "JSONB",
            "CATALOG": "JSONB",
            "LINK": "JSONB",
        }

    # тут верну список id датасетов, прошедших фильтр по ключевым словам (метро, мцд, мцк)
    def _target_ids(self) -> list[int]:
        ids = self.parser.get_dataset_ids()  # все id из категории 102
        return [
            ds for ds in ids
            if self.parser.KEYWORDS.search(
                (self.parser.get_dataset_meta(ds).get("Name") or "")
            )
        ]

    def _get_fields(self, ds_id: int) -> list[dict]:
        """
        Получает метаданные полей для датасета, дабы вернуть список словарей вида {'name': <поле>, 'type': <тип>}, чтоб на основе этого построить запрос на insert
        """
        meta = self.parser.get_dataset_meta(ds_id)  # Запрашиваем metadata
        raw = meta.get("Fields") or meta.get("Columns") or meta.get("Structure") or []
        fields = []

        for f in raw:
            # название поля
            name = f.get("Name") or f.get("ColumnName") or f.get("FieldName")

            # тип данных
            t = (f.get("Type") or f.get("DataType") or "STRING").strip().upper()

            # маппинг в SQL тип, по умолчанию текст
            fields.append({"name": name, "type": self.TYPE_MAP.get(t, "TEXT")})

        return fields

    def _create_table(self, ds_id: int, ds_name: str, fields: list[dict]) -> str:
        """
        Создаёт таблицу для данного датасета;
        Каждому полю присваивает SQL тип, который выяснен выше;
        Если найдены поля longitude_wgs84 и latitude_wgs84, добавляет столбец geom geometry;
        Возвращает имя созданной или уже существующей таблицы.
        """
        slug = slugify(ds_name)
        table = f"dataset_{ds_id}_{slug}"

        # Проверяем, существует ли таблица
        self.cur.execute("SELECT to_regclass(%s);", (table,))
        if self.cur.fetchone()[0]:
            return table

        cols = []
        has_lon = has_lat = False

        for fld in fields:
            nm, tp = fld["name"], fld["type"]
            cols.append(f'"{nm}" {tp}')

            if nm.lower() == "longitude_wgs84":
                has_lon = True
            if nm.lower() == "latitude_wgs84":
                has_lat = True
        # Если оба поля координат есть, добавляем столбец geom
        if has_lon and has_lat:
            cols.append("geom geometry(Point,4326)")

        # Формируем и выполняем ddl
        ddl = f"CREATE TABLE {table} (\n    " + ",\n    ".join(cols) + "\n);"
        self.cur.execute(ddl)
        self.pg.commit()
        return table

    def _fetch_features(self, ds_id: int) -> list[dict]:
        # requestим все geoJSON фичи из эндпоинта /v1/features/{ds_id}.
        url = f"https://apidata.mos.ru/v1/features/{ds_id}"
        resp = self.http.get(url, timeout=60)
        # если статус != 200, возвращает пустой список;
        if resp.status_code != 200:
            return []
        # иначе возвращает список объектов из ключа features
        return resp.json().get("features") or []

    def _insert(self, table: str, fields: list[dict], feats: list[dict]):
        """
        Вставляет батчем все фичи в таблицу, а для этого
        > добавляем имена колонок из списка полей;
        > если есть longitude_wgs84 и latitude_wgs84, добавляет колонку geom; """

        col_names = [f'"{fld["name"]}"' for fld in fields]

        has_lon = any(fld["name"].lower() == "longitude_wgs84" for fld in fields)
        has_lat = any(fld["name"].lower() == "latitude_wgs84" for fld in fields)

        if has_lon and has_lat:
            col_names.append("geom")

        cols_sql = "(" + ",".join(col_names) + ")"
        insert_sql = f"INSERT INTO {table} {cols_sql} VALUES %s"

        rows = []
        for f in feats:
            attrs = f.get("properties", {}).get("attributes", {})
            row = []
            lon = lat = None
            for fld in fields:
                nm, tp = fld["name"], fld["type"]
                val = attrs.get(nm)
                # сериализуем вложенные структуры в строку JSON
                if tp == "JSONB":
                    val = json.dumps(val or {})
                # Конвертация координат в float
                if nm.lower() == "longitude_wgs84":
                    try:
                        lon = float(val) if val is not None else None
                        val = lon
                    except:
                        val = None
                if nm.lower() == "latitude_wgs84":
                    try:
                        lat = float(val) if val is not None else None
                        val = lat
                    except:
                        val = None
                row.append(val)

            # Если есть координаты, добавляем WKBточку в geom
            if has_lon and has_lat:
                if lon is not None and lat is not None:
                    wkb = Point(lon, lat).wkb_hex
                    row.append(psycopg2.Binary(bytes.fromhex(wkb)))
                else:
                    row.append(None)

            rows.append(tuple(row))

        execute_values(self.cur, insert_sql, rows)
        self.pg.commit()

    def run(self):
        """
        Основной метод:
        1) Получаем список целевых датасетов;
        2) Для каждого ID:
           a] Получаем имя и список полей;
           b] Создаём таблицу;
           c] Запрашиваем фичи;
           d] Вставляем их в таблицу;
        3) Закрываем соединение после обработки всех датасетов.
        """
        targets = self._target_ids()
        for ds in targets:
            name = self.parser.get_dataset_meta(ds).get("Name") or f"dataset_{ds}"
            fields = self._get_fields(ds)
            table = self._create_table(ds, name, fields)
            feats = self._fetch_features(ds)
            if not feats:
                continue
            self._insert(table, fields, feats)
        self.cur.close()
        self.pg.close()
