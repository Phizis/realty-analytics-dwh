import os
from dotenv import load_dotenv, set_key

# Файл .env
ENV_FILE = ".env"

# Загрузка переменных окружения
load_dotenv(ENV_FILE)

def update_api_token():
    # Запрос API-токена у пользователя
    api_token = input("Enter API-token: ").strip()
    if not api_token:
        print("API-token is empty.")
        return False

    # Читаем текущее содержимое .env файла
    with open(ENV_FILE, "r") as file:
        lines = file.readlines()

    # Обновляем или добавляем API_TOKEN без кавычек
    with open(ENV_FILE, "w") as file:
        token_found = False
        for line in lines:
            if line.startswith("API_TOKEN="):
                file.write(f"API_TOKEN={api_token}\n")
                token_found = True
            else:
                file.write(line)
        if not token_found:
            file.write(f"API_TOKEN={api_token}\n")

    # Обновляем переменную окружения, чтобы новый токен был доступен немедленно
    os.environ["API_TOKEN"] = api_token

    print("API-token updated.")
    return True

import subprocess

def main():
    # Обновление API-токена
    if not update_api_token():
        return

    # Запуск основного скрипта users_auto.py
    print("Launch users update")
    try:
        result_users = subprocess.run(
            ["python", "C:\\Users\\path\\users_auto.py"],
            check=True,
            text=True,
            capture_output=True
        )
        print(result_users.stdout)  # Вывод стандартного потока
        print(result_users.stderr)  # Вывод ошибок
    except subprocess.CalledProcessError as e:
        print(f"Error executing users_auto.py: {e}")
        return  # Прерываем выполнение, если возникла ошибка

    print("Update users ended") 
    
    #....
    # Запуск следующего скрипта для обновления соответствующей таблицы      

if __name__ == "__main__":
    main()