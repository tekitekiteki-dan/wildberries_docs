import requests
import pandas as pd
from datetime import datetime
import time
import json
from src.api_client import ApiClient
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

with open('openapi.json', 'rb') as f:
    cnf = json.load(f)

token = os.environ['WB_token_stat']

client = ApiClient(token = token, cnf = cnf)

reports = [('incomes', 'Поставки'),
           ('stocks', 'Склад'),
           ('orders', 'Заказы'),
           ('sales', 'Продажи'),
           ('reportDetailByPeriod', 'Отчет о продажах по реализации')]

def save_data(method: str, report: str):
    data = client.__getattribute__(method).__call__()
    data.to_excel(f'{report}.xlsx', index = False)
    return

with ThreadPoolExecutor() as executor:
    futures = []
    for method, report in reports:
        futures.append(
            executor.submit(save_data, method, report)
            )

# for method, report in reports:
#     data = client.__getattribute__(method).__call__()
#     data.to_excel(f'{report}.xlsx', index = False)

client.session.close()