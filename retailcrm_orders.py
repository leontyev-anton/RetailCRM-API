import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas
import pandas_gbq
import sys
import time
from config import g_file, g_project, g_table_name, api_auth


def write_data(orders):
    orders_bq = []
    for order in orders:
        row = {'items': order['items'], 'summ':order['summ'], 'totalSumm':order['totalSumm'],
               'status': order['status'], 'createdAt':order['createdAt'], 'id': order['id']}
        orders_bq.append(row)

    try:
        credentials = service_account.Credentials.from_service_account_file(g_file, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        client = bigquery.Client(credentials=credentials, project=g_project)
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition='WRITE_TRUNCATE', ignoreUnknownValues=True)
        client.load_table_from_json(orders_bq, g_table_name, job_config=job_config).result()
    except Exception as e:
        print(f'Error. Can\'t write table in BigQuery: {e}')
    else:
        print(f'Table in BigQuery ' + g_table_name + ' is successfully created. Number of rows: {len(orders_bq)}')

    # # пишет неправильно колонку items
    # try:
    #     df = pandas.DataFrame.from_dict(orders)
    #     pandas_gbq.to_gbq(df, g_table_name, project_id=g_project, if_exists='replace')
    # except Exception as e:
    #     print(f'Error. Can\'t write table in BigQuery: {e}')
    # else:
    #     print(f'Table in BigQuery {g_table_name} is successfully created. Number of rows: {len(df)}')


retail_url = 'https://v-import.retailcrm.ru/api/v5/orders'  # filter[numbers][]=1235C&filter[customFields][nps][min]=5
limit_url = '?limit=100'  # 20|50|100 only                  # https://help.retailcrm.ru/Developers/Index

try:
    response = requests.get(retail_url + limit_url + '&page=1' + api_auth)
    #print(f'Response Status Code = {response.status_code}'); print(f'Response Headers = {response.headers}'); print(f'Response Text = {response.text}');
    success = response.json()['success']
    pagination = response.json()['pagination']
    limit = int(pagination['limit'])
    currentPage = int(pagination['currentPage'])
    totalCount = int(pagination['totalCount'])
    totalPageCount = int(pagination['totalPageCount'])
    orders = response.json()['orders']
except Exception as e:
    print(f'Error at first request: {e}')
else:
    print(f'First response: currentPage:{currentPage}, success:{success}, limit:{limit}, totalCount:{totalCount}, totalPageCount:{totalPageCount}, len orders:{len(orders)}')
    if success == True and totalCount > 0 and len(orders) > 0:
        currentPage += 1
        while currentPage <= totalPageCount:  # распарсим следующие страницы, если их больше одной
            try:
                response1 = requests.get(retail_url + limit_url + '&page=' + str(currentPage) + api_auth)
                success1 = response1.json()['success']
                orders1 = response1.json()['orders']
            except Exception as e:
                print(f'Error at one of next requests: {e}')
                sys.exit(1)
            else:
                print(f'Next responses: currentPage:{currentPage}, success1:{success1}, len orders1:{len(orders1)}. ', end='')
                if success1 == True and len(orders1) > 0:
                    orders += orders1
                    print('Success append list')
                else:
                    print('Error, don\'t append list')
            time.sleep(0.15)  # ограничение API - 10 запросов в секунду. оставим небольшой запас
            currentPage += 1
        if (totalCount == len(orders)):
            write_data(orders)
        else:
            print('Something was wrong. Won\'t write data to database')
    else:
        print('First request is empty or not correct')
