from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from utils import PROJECT_ID, TABLE_ID_EMPLOYEES, TABLE_ID_DEPARTMENTS, TABLE_ID_JOBS, CREDENTIALS_PATH

if __name__ == '__main__':
    
    print("This program will restore bq tables to their original state. PLease be sure to have the avro files for backup.")
    print("Continue? (y/n)")
    
    ok =  input()
    
    if ok=='y':

        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

        hired_employees =  pd.read_csv('./initial_data/hired_employees.csv')

        jobs = pd.read_csv('./initial_data/jobs.csv')

        departments = pd.read_csv('./initial_data/departments.csv')

        hired_employees.columns = ['id','name','datetime', 'department_id', 'job_id']
        departments.columns = ['id', 'department']
        jobs.columns = ['id', 'job']
        print( hired_employees)
        print(jobs)
        print(departments)
        
        print("Uploading hired_employees!")
        hired_employees.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_EMPLOYEES, progress_bar= True, credentials=credentials, if_exists='replace')
        print("Uploading jobs!")
        jobs.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_JOBS, progress_bar= True, credentials=credentials, if_exists='replace')
        print("Uploading departments!")
        departments.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_DEPARTMENTS, progress_bar= True, credentials=credentials, if_exists='replace')

        print("Upload Succesful")
    else:
        print("Not restoring bigquery database")
