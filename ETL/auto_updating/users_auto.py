# -*- coding: utf-8 -*-
import os
import logging
from dotenv import load_dotenv
import requests
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, JSON, Date, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

# Настройка логирования
log_file_path = os.path.join(os.getcwd(), "app.log")  # Явный путь к файлу логов
try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, encoding="utf-8"),  # Указываем кодировку
            logging.StreamHandler()                                # Вывод в консоль
        ]
    )
except Exception as e:
    print(f"Error setting up logging: {e}")

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройки базы данных
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Определение модели таблицы users
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)  # Первичный ключ
    full_name = Column(String, index=True)  # ФИО пользователя
    phone = Column(String, nullable=True)  # Телефон
    email = Column(String, nullable=True)  # Email
    company = Column(String, nullable=True)  # Компания
    department = Column(String, nullable=True)  # Отдел
    position = Column(String, nullable=True)  # Должность
    birth_date = Column(Date, nullable=True)  # Дата рождения
    start_date = Column(Date, nullable=True)  # Дата начала работы
    gender = Column(String, nullable=True)  # Пол
    role = Column(String, nullable=True)  # Роли
    accepted_by = Column(String, nullable=True)  # Автор
    office = Column(String, nullable=True)  # Офис
    address = Column(Text, nullable=True)  # Адрес
    is_active = Column(Boolean, nullable=True)  # Доступ в систему
    activation_date = Column(Date, nullable=True)  # Дата активации
    ads_bonus = Column(Integer, nullable=True)  # Рекламные бонусы   
    career_start = Column(Date, nullable=True)  # Дата начала карьеры
    deactivation_date = Column(Date, nullable=True)  # Дата деактивации

# Определение модели таблицы webhook_data
class WebhookData(Base):
    __tablename__ = "webhook_data"
    id = Column(Integer, primary_key=True, index=True)  # Первичный ключ (это user_id)
    received_at = Column(String, nullable=False)  # Время получения
    event = Column(String, nullable=False)  # Событие
    app = Column(String, nullable=False)  # Приложение
    url = Column(String, nullable=False)  # URL
    process = Column(Boolean, nullable=True)  # Обработано ли событие

# Функция для сохранения данных в БД
def save_user_to_db(user_data):
    db = SessionLocal()
    try:
        existing_user = db.query(User).filter(User.id == user_data["id"]).first()
        if existing_user:
            for key, value in user_data.items():
                setattr(existing_user, key, value)
            logging.info(f"User with ID {user_data['id']} updated in the database.")
        else:
            new_user = User(**user_data)
            db.add(new_user)
            logging.info(f"New user with ID {user_data['id']} added to the database.")
        db.commit()
    except Exception as e:
        logging.error(f"Error saving user to database: {e}")
        db.rollback()
    finally:
        db.close()

# Функция для получения данных из API
def fetch_and_process_data(app, user_id, event, webhook_id):
    API_BASE_URL = os.getenv("API_BASE_URL")
    API_TOKEN = os.getenv("API_TOKEN").strip()
    #logging.info(f"Using API token: {API_TOKEN}")
    API_ACCEPT_HEADER = os.getenv("API_ACCEPT_HEADER")

    if app == 'user':
        url = f"{API_BASE_URL}/{app}/{user_id}"
    headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": API_ACCEPT_HEADER
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Received data from API for user ID {user_id}: {data}")
        user_data = data.get('data', {})
        if not user_data:
            logging.warning(f"No user data found in the response for user ID {user_id}.")
            return
        parsed_data = {
            "id": user_data.get("id"),
            "full_name": user_data.get("title"),
            "phone": None,
            "email": None,
            "company": None,
            "department": None,
            "position": None,
            "birth_date": None,
            "start_date": None,
            "gender": None,
            "role": None,
            "accepted_by": None,
            "office": None,
            "address": None,
            "is_active": None,
            "activation_date": None,
            "ads_bonus": None,
            "career_start": None,
            "deactivation_date": None
        }
        
        fields = user_data.get("fields", [])
        for field in fields:
            field_label = field.get("external_id")
            values = field.get("values", [])

            if field_label == "phone" and values:
                parsed_data["phone"] = values[0].get("value")
            elif field_label == "email" and values:
                parsed_data["email"] = values[0].get("value")
            elif field_label == "company" and values:
                parsed_data["company"] = values[0]["value"]["title"]
            elif field_label == "department" and values:
                parsed_data["department"] = values[0]["value"]["title"]
            elif field_label == "position" and values:
                parsed_data["position"] = values[0]["value"]["title"]
            elif field_label == "birthday" and values:
                parsed_data["birth_date"] = values[0]["value"]["date"]
            elif field_label == "wds" and values:
                parsed_data["start_date"] = values[0]["value"]["date"]
            elif field_label == "gender" and values:
                parsed_data["gender"] = values[0]["text"]
            elif field_label == "roles" and values:
                parsed_data["role"] = [role["value"]["title"] for role in values]
            elif field_label == "author_id" and values:
                parsed_data["accepted_by"] = values[0]["value"]["title"]
            elif field_label == "office" and values:
                parsed_data["office"] = values[0]["value"]["title"]
            elif field_label == "address" and values:
                parsed_data["address"] = values[0]["location"]["full_address_text"]
            elif field_label == "active" and values:
                parsed_data["is_active"] = values[0]["value"]
            elif field_label == "date_activation" and values:
                parsed_data["activation_date"] = values[0]["value"]["date"]
            elif field_label == "balance_market" and values:
                parsed_data["ads_bonus"] = values[0]["value"]
            elif field_label == "date_start_career" and values:
                parsed_data["career_start"] = values[0]["value"]["date"]
            elif field_label == "date_deactivation" and values:
                parsed_data["deactivation_date"] = values[0]["value"]["date"]

        # Сохраняем или обновляем данные в зависимости от события
        if event == "update":
            logging.info(f"Updating user with ID {user_id}...")
        elif event == "create":
            logging.info(f"Inserting new user with ID {user_id}...")
        save_user_to_db(parsed_data)
        logging.info("Data saved to the database.")
        # Помечаем запись как обработанную
        mark_webhook_as_processed(webhook_id)
    else:
        logging.error(f"Error fetching data from API for user ID {user_id}: Status code {response.status_code}")

# Функция для пометки записи как обработанной
def mark_webhook_as_processed(webhook_id):
    db = SessionLocal()
    try:
        webhook = db.query(WebhookData).filter(WebhookData.id == webhook_id).first()
        if webhook:
            webhook.process = True
            db.commit()
            logging.info(f"Webhook with ID {webhook_id} marked as processed.")
    except Exception as e:
        logging.error(f"Error marking webhook as processed: {e}")
        db.rollback()
    finally:
        db.close()

# Функция для обработки всех записей в таблице webhook_data
def process_webhook_data():
    db = SessionLocal()
    try:
        all_webhook_data = db.query(WebhookData).all()
        for webhook in all_webhook_data:
            if webhook.process:  # Пропускаем уже обработанные записи
                logging.info(f"Skipping webhook with ID {webhook.id} because it is already processed.")
                continue
            user_id = webhook.id
            app = webhook.app  # Приложение
            event = webhook.event  # Событие
            webhook_id = webhook.id  # ID записи в таблице webhook_data
            if app != 'user':
                logging.info(f"Skipping webhook with ID {webhook_id} because app is not 'user'.")
                continue
            fetch_and_process_data(app, user_id, event, webhook_id)
    except Exception as e:
        logging.error(f"Error processing webhook data: {e}")
    finally:
        db.close()

# Пример использования
if __name__ == "__main__":
    try:
        print("Starting script execution...")
        process_webhook_data()
        print("Script execution completed.")
    except Exception as e:
        print(f"Error: {e}")