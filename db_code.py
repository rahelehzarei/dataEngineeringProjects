import sqlite3
import pandas as pd

conn = sqlite3.connect('STAFF.db')

tabel_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = './INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

df.to_sql(tabel_name, conn, if_exists = 'replace', index = False)
print('table is ready')

# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {tabel_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {tabel_name}"
query_output= pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {tabel_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(tabel_name, conn, if_exists= 'append', index = False)
print('Data appended successfully')

query_statement = f"SELECT COUNT(*) FROM {tabel_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()