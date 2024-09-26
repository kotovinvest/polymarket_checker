import requests
import pandas as pd
import random
import logging
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функция для чтения данных из файла
def read_file(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Функция для проверки поддержки HTTPS-прокси
def test_https_proxy(proxy):
    try:
        proxy_dict = {
            "https": f"http://{proxy}"  # Используем http для запросов через https
        }
        test_url_https = "https://httpbin.org/ip"  # Проверка через HTTPS
        
        response = requests.get(test_url_https, proxies=proxy_dict, timeout=10)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False

# Функция для выполнения запроса к API с использованием прокси
def get_response_with_proxy(url, proxies, retries=10):
    used_proxies = set()  # Множество для хранения использованных прокси
    
    for _ in range(retries):
        if len(used_proxies) == len(proxies):
            return None

        # Выбираем случайный прокси, который еще не использовали
        proxy = random.choice([p for p in proxies if p not in used_proxies])
        used_proxies.add(proxy)
        
        if not test_https_proxy(proxy):  # Проверяем прокси на поддержку HTTPS перед использованием
            continue
        
        proxy_dict = {
            "https": f"http://{proxy}"
        }
        
        try:
            response = requests.get(url, proxies=proxy_dict, timeout=30)  # Увеличиваем таймаут до 30 секунд
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            sleep(2)  # Задержка между переключениями прокси
    
    return None

# Основная функция для получения данных по кошельку
def get_wallet_data(wallet, proxies):
    logging.info(f"Обрабатываем кошелек: {wallet}")
    
    # API 1: получение данных с первого API
    url_1 = f"https://lb-api.polymarket.com/rank?window=all&rankType=vol&address={wallet}"
    data_1 = get_response_with_proxy(url_1, proxies)
    
    # API 2: получение данных со второго API
    url_2 = f"https://data-api.polymarket.com/value?user={wallet}"
    data_2 = get_response_with_proxy(url_2, proxies)
    
    # API 3: получение данных с третьего API
    url_3 = f"https://api.rabby.io/v1/user/total_balance?id={wallet}"
    data_3 = get_response_with_proxy(url_3, proxies)

    # Обработка данных с 1-го и 2-го API, если они есть, иначе задаем 0
    name = "N/A"
    rank = 0
    total_volume = 0
    сумма_открытых_ставок = 0
    
    if data_1 and len(data_1) > 0:
        name = data_1[0].get("name", "N/A")
        rank = data_1[0].get("rank", 0)
        total_volume = data_1[0].get("amount", 0)
    
    if data_2 and len(data_2) > 0:
        сумма_открытых_ставок = data_2[0].get("value", 0)

    # Даже если первые два API не вернули данные, пытаемся получить данные с третьего API
    balance = 0
    if data_3 and "total_usd_value" in data_3:
        balance = data_3.get("total_usd_value", 0)

    # Возвращаем результат для каждого кошелька
    return {
        "wallet": wallet,
        "name": name,
        "rank": rank,
        "total volume": total_volume,  # Изменено
        "сумма открытых ставок": сумма_открытых_ставок,  # Изменено
        "balance": balance  # Изменено
    }

# Основная программа
def main():
    # Чтение данных из файлов
    wallets = read_file('wallets.txt')
    proxies = read_file('proxy.txt')
    
    # Список для хранения данных в порядке оригинального списка
    data_list = [None] * len(wallets)  # Инициализация списка размером с количество кошельков
    
    # Используем ThreadPoolExecutor для многопоточности
    with ThreadPoolExecutor(max_workers=10) as executor:  # Максимальное количество потоков
        futures = {executor.submit(get_wallet_data, wallets[i], proxies): i for i in range(len(wallets))}
        
        for future in as_completed(futures):
            index = futures[future]  # Получаем индекс
            data = future.result()
            data_list[index] = data  # Сохраняем данные по индексу

    # Преобразуем данные в DataFrame
    df = pd.DataFrame(data_list)
    
    # Сохранение в Excel
    df.to_excel('wallets_data.xlsx', index=False)
    logging.info("Данные успешно сохранены в wallets_data.xlsx")

if __name__ == '__main__':
    main()
