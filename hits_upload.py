from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Date, Float
import pandas as pd

# путь к csv файлу
csv_file_path = "C:/Users/user/airflow_diploma/data/ga_hits.csv"

# подключение к базе данных
engine = create_engine("postgresql://postgres:motorbreath12@localhost:5432/sber_auto")

# загружаем файл hits
df = pd.read_csv(csv_file_path)

# создаём таблицу hits в базе данных sber_auto
df.to_sql(
    "hits",
    engine,
    if_exists='append',   # не включаем индексы в таблицу
    index=False,   # добавляем новые данные к уже существующим
    dtype={   # задаём типы данных колонок
        "session_id": Text,
        "hit_date": Date,
        "hit_time": Float,
        "hit_number": Integer,
        "hit_type": Text,
        "hit_referer": Text,
        "hit_page_path": Text,
        "event_category": Text,
        "event_action": Text,
        "event_label": Text,
        "event_value": Text
    }
)

print("Данные успешно загружены в таблицу hits")
