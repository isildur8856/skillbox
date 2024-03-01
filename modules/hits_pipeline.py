import json
import os
import psycopg2

# функция чтения файла json
def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

# функция подключения к базе данных
def connect_to_postgres():
    conn = psycopg2.connect(
        dbname="sber_auto",
        user="postgres",
        password="motorbreath12",
        host="localhost",
        port="5432"
    )
    return conn

# функция записи новых данных в таблицу hits
def insert_data_to_postgres(conn, data):
    cursor = conn.cursor()
    try:
        for date, entries in data.items():
            for entry in entries:
                cursor.execute("""
                        INSERT INTO hits    
                    (session_id, hit_date, hit_time, hit_number, hit_type,
                    hit_referer, hit_page_path, event_category, event_action,
                    event_label, event_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    entry['session_id'], date, entry['hit_time'], entry['hit_number'],
                    entry['hit_type'], entry['hit_referer'], entry['hit_page_path'],
                    entry['event_category'], entry['event_action'], entry['event_label'],
                    entry['event_value']
                ))
        conn.commit()
        print("Данные успешно добавлены в таблицу hits.")
    except psycopg2.Error as e:   # механизм разрешения конфликтов
        print("Ошибка при добавлении данных в таблицу hits:", e)
        conn.rollback()
    finally:
        cursor.close()


def hits_main():
    json_folder = "C:/Users/user/airflow_diploma/data/extra_hits"
    json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
    conn = connect_to_postgres()

    for json_file in json_files:
        file_path = os.path.join(json_folder, json_file)
        data = read_json_file(file_path)
        insert_data_to_postgres(conn, data)

    conn.close()


if __name__ == "__main__":
    hits_main()