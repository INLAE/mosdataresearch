import os
import re
import requests
from typing import List, Dict, Any, Optional


class MosTransportParser:
    BASE_URL = "https://apidata.mos.ru/v1"
    # регулярка ищет упоминания в названий датасетов метро / МЦД / МЦК и любые формы слов диаметр... и кольц...
    KEYWORDS = re.compile(r"(метро"
                          r"|мцд"
                          r"|мцк"
                          r"|диаметр\w*"
                          r"|кольц\w*)",
                          re.IGNORECASE | re.UNICODE)

    def __init__(self, api_key: str, outfile: str = "dataset_researh.txt"):
        self.session = requests.Session()
        self.session.params = {"api_key": api_key}  # как в постмане клеим ключ в каждый запрос
        self.outfile = outfile

    # Получить id всех датасетов категории 102
    def get_dataset_ids(self, category_id: int = 102) -> List[int]:
        # Готовим параметры OData для фильтра по id категории
        params = {"$filter": f"Id eq {category_id}", "$top": 1}
        # Запрашиваем список категорий
        resp = self.session.get(
            f"{self.BASE_URL}/categories",
            params=params,
            timeout=30,
        )
        data = resp.json()  # Парсим json

        # Непонятно, но может прийти в разных обертках - разбираем все варианты
        cats = data.get("categories") or data.get("value") or data
        return cats[0].get("Datasets", [])  # Возвращаем массив id наборов

    # Получить метаданные одного набора
    def get_dataset_meta(self, ds_id: int) -> Dict[str, Any]:
        resp = self.session.get(f"{self.BASE_URL}/datasets/{ds_id}",
                                timeout=30)  # Запрос метаданных
        resp.raise_for_status()  # Проверяем статус ответа
        return resp.json()  # Отдаем словарь

    # Извлечь список полей из метаданных
    def _fields(self, meta: Dict[str, Any]) -> List[Dict[str, Any]]:
        # В разных наборах секция со столбцами может называться по-разному
        return meta.get("Fields") or meta.get("Columns") or meta.get("Structure") or []

    # Сохранить отфильтрованные наборы в файл
    def dump_filtered(self, ids: List[int]) -> None:
        with open(self.outfile, "w", encoding="utf-8") as f:
            for ds_id in ids:  # Итерируем по каждому id набора
                try:
                    meta = self.get_dataset_meta(ds_id)  # Грузим метаданные
                except requests.HTTPError as e:  # Обрабатываем HTTP errs
                    print(f"[{ds_id}] HTTP {e.response.status_code}")
                    continue  # Переходим к следующему набору

                # Получаем название набора (иногда хранится под другим ключом)
                name = meta.get("Name") or meta.get("Caption") or meta.get("Title") or ""
                if not self.KEYWORDS.search(name):  # Проверяем, содержит ли название ключевые слова
                    continue  # Если нет - пропускаем
                f.write(f"=== Dataset {ds_id}: {name}\n")  # Пишем заголовок датасета
                f.write("=" * 100 + "\n")
                for fld in self._fields(meta):  # Перебираем поля набора
                    fname = fld.get("Name") or fld.get("ColumnName") or fld.get("FieldName")
                    ftype = fld.get("Type") or fld.get("DataType")
                    f.write(f" • {fname} ({ftype})\n")  # Записываем имя и тип поля
                f.write("\n")
        print(f"Сохранено в {self.outfile}")


if __name__ == "__main__":
    key = os.getenv("MOS_API_KEY")  # Читаем apikey из переменной окружения
    parser = MosTransportParser(key)
    ids = parser.get_dataset_ids()  # Получаем все id в категории 102
    print(f"Всего наборов в категории 102: {len(ids)}")  # А сколько?
    parser.dump_filtered(ids)
