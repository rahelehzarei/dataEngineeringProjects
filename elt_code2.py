import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datatime import datatime

log_file = 'log_file.txt'
target_fle = 'transformed_data.csv'

#load 

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe  =pd.read_json(file_to_process, lines = True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.dataframe(columns=['name', 'height', 'weight'])
    tree = ET.parse(file_to_process)
    root = tree.get_root()

    for person in persons:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{'name': name, 'height': height, 'weight': weight}])], ignore_index = True)

    return dataframe

# extraction

def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(pf.read_from_csv(csvfile))], ignore_index = True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(pf.read_from_json(jsonfile))], ignore_index = True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(pf.read_from_xml(xmlfile))], ignore_index = True)

    return extracted_data

# transformation

def transofrm(date):
    data['height'] = round(data.height * 0.0254, 2)
    data['weight'] = round(data.weight * 0.4535, 2)

    return data

# load and log

def load_data(target_fle, transformed_data):
    transformed_data.to_csv(target_fle)

def log_process(message):
    timeformat = '%Y-%h-%d-%H:%M:%S'
    now = datatime.now() 
    timestamp = now.strftime(timeformat)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' , ' + message + '\n')



# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 