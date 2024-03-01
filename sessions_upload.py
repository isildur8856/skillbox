import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Date, Time

# подключение к базе данных
engine = create_engine("postgresql://postgres:motorbreath12@localhost:5432/sber_auto")

# путь к csv файлу
csv_file_path = "C:/Users/user/airflow_diploma/data/ga_sessions.csv"

# загружаем файл sessions
df = pd.read_csv(csv_file_path)

# добавляем таблицу sessions в базу данных sber_auto
df.to_sql(
    "sessions",
    engine,
    index=False,   # не включаем индексы в таблицу
    if_exists='append',    # добавляем новые данные к уже существующим
    dtype={   # задаём типы данных колонок
        "session_id": Text,
        "client_id": Text,
        "visit_date": Date,
        "visit_time": Time,
        "visit_number": Integer,
        "utm_source": Text,
        "utm_medium": Text,
        "utm_campaign": Text,
        "utm_adcontent": Text,
        "utm_keyword": Text,
        "device_category": Text,
        "device_os": Text,
        "device_brand": Text,
        "device_model": Text,
        "device_screen_resolution": Text,
        "device_browser": Text,
        "geo_country": Text,
        "geo_city": Text
    }
)

print("Данные успешно загружены в таблицу sessions")