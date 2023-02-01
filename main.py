import requests
import pandas as pd
from datetime import datetime
import time
import json
from src.api_client import ApiClient
import os
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

for method, report in reports:
    data = client.__getattribute__(method).__call__()
    data.to_excel(f'{report}.xlsx', index = False)