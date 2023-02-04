import requests
import pandas as pd
from datetime import datetime
import time

class ApiClient:
    def __init__(self, token: str, cnf: dict):
        self.token = token
        self.cnf = cnf
        self.url_stat = 'https://statistics-api.wildberries.ru'
        self.session = requests.session()
        self.session.headers.update({'Authorization': self.token})
    
    def __request_get(self, path: str, params: dict):
        while True:
            try:
                r = self.session.get(url = self.url_stat + path, params = params, timeout = 60)
                if r.status_code != 200:
                    if r.json()['errors'] == ['(api-new) too many requests']:
                        time.sleep(60)
                    else:
                        break
                else:
                    break
            except:
                time.sleep(10)
        if r.status_code != 200:
            raise Exception(r.json()['errors'])
        r = r.json()
        r = pd.DataFrame(r)
        
        return r
    
    def __rename_columns(self, data: pd.DataFrame, path: str):
        cols = self.cnf['paths'][path]['get']['responses']['200']['content']\
            ['application/json']['schema']['items']['$ref']
        x = self.cnf
        for c in cols.split('/')[1:]:
            x = x[c]
        cols = x['properties']
        for i, c in enumerate(data.columns):
            try:
                data = data.rename(columns = {c: cols[c]['description'].split('.')[0]})
            except:
                pass
        
        return data
    
    def incomes(self,
                dateFrom: str = '2019-06-20',
                is_russian: bool = True):
        params = locals()
        params.pop('self', 'is_russian')
        path = '/api/v1/supplier/incomes'
        data = self.__request_get(path = path, params = params)
        if is_russian:
            data = self.__rename_columns(data = data, path = path)
        return data
    
    def stocks(self,
               dateFrom: str = '2019-06-20',
               is_russian: bool = True):
        params = locals()
        params.pop('self', 'is_russian')
        path = '/api/v1/supplier/stocks'
        data = self.__request_get(path = path, params = params)
        if is_russian:
            data = self.__rename_columns(data = data, path = path)
        return data
    
    def orders(self,
               dateFrom: str = '2019-06-20',
               is_russian: bool = True):
        params = locals()
        params.pop('self', 'is_russian')
        path = '/api/v1/supplier/orders'
        data = self.__request_get(path = path, params = params)
        if is_russian:
            data = self.__rename_columns(data = data, path = path)
        return data
    
    def sales(self,
              dateFrom: str = '2019-06-20',
              is_russian: bool = True):
        params = locals()
        params.pop('self', 'is_russian')
        path = '/api/v1/supplier/sales'
        data = self.__request_get(path = path, params = params)
        if is_russian:
            data = self.__rename_columns(data = data, path = path)
        return data
    
    def reportDetailByPeriod(self,
                             dateFrom: str = '2019-06-20',
                             dateTo: str = str(datetime.now().date()),
                             is_russian: bool = True):
        params = locals()
        params.pop('self', 'is_russian')
        params['rrdid'] = 0
        path = '/api/v1/supplier/reportDetailByPeriod'
        data = pd.DataFrame()
        while True:
            _data = self.__request_get(path = path, params = params)
            if _data.shape[0] > 0:
                params['rrdid'] = _data['rrd_id'].iloc[-1]
                if is_russian:
                    _data = self.__rename_columns(_data, path = path)
                data = pd.concat([data, _data])
            else:
                break
        return data

if __name__ == 'main':
    pass