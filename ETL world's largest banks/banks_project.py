import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup


url = '	https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'
table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']

# log each step of the process
def log_progress(message):
    dt_format = '%Y-%h-%d-%H:%M:%s'
    now = datetime.now()
    now_format = now.strftime(dt_format)
    with open('code_log.txt', 'a') as f:
        f.write(now_format + ': ' + message + '\n' )

def extract(url, table_attribs):
    page = requests.get(url).text
    HTML = BeautifulSoup(page, 'html.parser')
    tables = HTML.find_all('tbody')
    largest_bank_table = tables[0]
    banks = largest_bank_table.find_all('tr')
    df = pd.DataFrame(columns = ['name', 'MC_USD_Billion'])
    # print(banks)
    for bank in banks:
        cols = bank.find_all('td')
        if len(cols) != 0:
            # print(bank)
            name = cols[1].find_all('a')[1].contents[0]
            MC_USD_Billion = cols[2].contents[0][:-1]
            data_dict = {'name': name, 'MC_USD_Billion': MC_USD_Billion}
            df_new = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df, df_new], ignore_index = True)
            # print(df)
    return df

def transform(df):
    rates = pd.read_csv(exchange_rate_csv_path)
    rate_dict = rates.set_index('Currency').to_dict()['Rate']
    # print(rate_dict)
    df['MC_GBP_Billion'] = [np.round(float(x)* rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] =  [np.round(float(x)* rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] =  [np.round(float(x)* rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    print(df)
    print('Market capitalization of the 5th largest bank in billion EUR \n')
    print(df['MC_EUR_Billion'][4])
    return df

def load_to_csv(df, csv_path):
        df.to_csv(csv_path)

def load_to_db(df, sql_conn, table_name):
    df.to_sql(table_name, sql_conn, if_exists= 'replace', index = False)

def run_query(sql_statement, sql_conn):
    print(sql_statement)
    print(pd.read_sql(sql_statement, sql_conn))


log_progress('Peliminaries complete, Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('banks.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * FROM {table_name};"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name};"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name FROM {table_name} LIMIT 5;"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed.')