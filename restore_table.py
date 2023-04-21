from fastavro import reader
from google.oauth2 import service_account
import pandas as pd
from utils import PROJECT_ID, TABLE_ID_EMPLOYEES,CREDENTIALS_PATH, TABLE_ID_JOBS,TABLE_ID_DEPARTMENTS
# 1. List to store the records

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

def restore_table(table_id, filename):

    """"Receives a table_id and restores it in bq."""

    avro_records = []

    with open(f'./backup/avro_files/{filename}.avro', 'rb') as file:
        avro_reader = reader(file)
        for record in avro_reader:
            avro_records.append(record)
            
    df_avro = pd.DataFrame(avro_records)

    print("Uploading table to bigquery")

    df_avro.to_gbq(project_id=PROJECT_ID, destination_table=table_id, progress_bar= True, credentials=credentials, if_exists='replace')

if __name__ == "__main__":

    print("Enter which table you want to restore:")

    table = input()
    if table == 'hired_employees':
        print("Insert date:")
        date = input() 
        restore_table(TABLE_ID_EMPLOYEES,f"{table}_{date}")
    elif table == 'departments':
        print("Insert date:")
        date = input()  
        restore_table(TABLE_ID_DEPARTMENTS,f"{table}_{date}")
    elif table == 'jobs': 
        print("Insert date:")
        date = input() 
        restore_table(TABLE_ID_JOBS,f"{table}_{date}")
    
    print(f"Table {table} restored succesfully!")