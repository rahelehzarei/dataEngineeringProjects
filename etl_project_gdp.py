
# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests
import pandas as pd
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
db_name = 'World_Economies.db'
table_attribs = ['Country', 'GDP_USD_millions']
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    data = requests.get(url).text
    HTML = BeautifulSoup(data, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = HTML.find_all('tbody')
    gdp_table = tables[2]
    rows = gdp_table.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            if cols[0].find('a') is not None and '—' not in cols[2]:
                data_dict = {'Country': cols[0].a.contents[0],
                            'GDP_USD_millions': cols[2].contents[0]}
                new_df = pd.DataFrame(data_dict, index = [0])
                df = pd.concat([df, new_df], ignore_index = True)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    gdp_list = df['GDP_USD_millions'].tolist()
    gdp_list = [float("".join(x.split(','))) for x in gdp_list]
    gdp_list = [np.round(x/1000, 2) for x in gdp_list]
    df['GDP_USD_millions'] = gdp_list
    df = df.rename(columns = {"GDP_USD_millions": "GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    ''' Here, you define the required entities and call the relevant 
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    dt_format = '%Y-%h-%d-%H:%M:%s'
    dt = datetime.now()
    timestamp = dt.strftime(dt_format)
    with open ('./etl_project_log.txt', 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')

log_progress('Peliminaries complete, Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()