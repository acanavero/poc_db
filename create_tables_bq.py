from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from utils import PROJECT_ID, TABLE_ID_EMPLOYEES, TABLE_ID_DEPARTMENTS, TABLE_ID_JOBS, CREDENTIALS_PATH
from utils import hired_employees_schema, departments_schema, jobs_schema

if __name__ == '__main__':
    
    print("This program will restore bq tables to their original state. PLease be sure to have the avro files for backup.")
    print("Continue? (y/n)")
    
    ok =  input()
    
    if ok=='y':

        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

        hired_employees =  pd.read_csv('./initial_data/hired_employees.csv', header= None)

        jobs = pd.read_csv('./initial_data/jobs.csv', dtype={"id":'int32', "job": "string"}, header = None)

        departments = pd.read_csv('./initial_data/departments.csv', dtype={"id":'int32', "department": "string"}, header = None)

        hired_employees.columns = ['id','name','datetime', 'department_id', 'job_id']
        hired_employees['name'] = hired_employees['name'].astype('str')
        departments.columns = ['id', 'department']
        jobs.columns = ['id', 'job']
        print(hired_employees)
        print(jobs)
        print(departments)
        
        print("Uploading hired_employees!")
        hired_employees.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_EMPLOYEES, progress_bar= True, credentials=credentials, if_exists='replace', table_schema=hired_employees_schema)
        print("Uploading jobs!")
        jobs.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_JOBS, progress_bar= True, credentials=credentials, if_exists='replace', table_schema= jobs_schema)
        print("Uploading departments!")
        departments.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_DEPARTMENTS, progress_bar= True, credentials=credentials, if_exists='replace', table_schema= departments_schema)

        print("Upload Succesful")
    else:
        print("Not restoring bigquery database")
