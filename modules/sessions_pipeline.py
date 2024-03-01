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

# функция записи новых данных в таблицу sessions
def insert_data_to_postgres(conn, data):
    cursor = conn.cursor()
    try:
        for date, entries in data.items():
            for entry in entries:
                cursor.execute("""
                        INSERT INTO sessions
                        (session_id, client_id, visit_date, visit_time, visit_number,
                        utm_source, utm_medium, utm_campaign, utm_adcontent,
                        utm_keyword, device_category, device_os, device_brand,
                        device_model, device_screen_resolution, device_browser,
                        geo_country, geo_city)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                    entry['session_id'], entry['client_id'], date, entry['visit_time'],
                    entry['visit_number'], entry['utm_source'], entry['utm_medium'], entry['utm_campaign'],
                    entry['utm_adcontent'], entry['utm_keyword'], entry['device_category'], entry['device_os'],
                    entry['device_brand'], entry['device_model'], entry['device_screen_resolution'],
                    entry['device_browser'], entry['geo_country'], entry['geo_city']
                ))
        conn.commit()
        print("Данные успешно добавлены в таблицу sessions.")
    except psycopg2.Error as e:   # механизм разрешения конфликтов
        print("Ошибка при добавлении данных в таблицу sessions:", e)
        conn.rollback()
    finally:
        cursor.close()


def sessions_main():
    json_folder = "C:/Users/user/airflow_diploma/data/extra_sessions/"
    json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
    conn = connect_to_postgres()

    for json_file in json_files:
        file_path = os.path.join(json_folder, json_file)
        data = read_json_file(file_path)
        insert_data_to_postgres(conn, data)

    conn.close()


if __name__ == '__main__':
    sessions_main()
