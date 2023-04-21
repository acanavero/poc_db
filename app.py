from flask import Flask, request
import pandas as pd
from utils import PROJECT_ID, CREDENTIALS_PATH, TABLE_ID_EMPLOYEES, TABLE_ID_DEPARTMENTS, TABLE_ID_JOBS
from bq_connector import generate_bq_table_schema
from data_validation import validate_columns, verify_null, parse_all_types
from google.oauth2 import service_account
import json


app = Flask(__name__)

@app.get("/api/employees_hired_2021")
def employees_hired_quarters():
    query = """SELECT * from hallowed-valve-370617.db_employees.hired_employees e
                INNER JOIN hallowed-valve-370617.db_employees.departments d on e.department_id = d.id
                INNER JOIN hallowed-valve-370617.db_employees.jobs j on e.job_id = j.id"""

@app.post("/api/insert/all_data")
def insert_all_data():
    """returs body msg if data could not be inserted by conditions"""
    
    body_msg = {}
    
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

    data = request.get_json()
    
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

    h_emp_df = pd.DataFrame.from_records(data["hired_employees"])
    dep_df = pd.DataFrame.from_records(data["departments"])
    jobs_df = pd.DataFrame.from_records(data["jobs"])

    a =  validate_columns(h_emp_df,dep_df,jobs_df) 

    if not verify_null(h_emp_df,dep_df,jobs_df): #if it doesn't have any null values

        body_msg = parse_all_types(h_emp_df, dep_df, jobs_df)
        
        print(h_emp_df.shape)
        
        if body_msg == True:
            print("Trying to Upload to bigquery")
            emp_schema = generate_bq_table_schema(TABLE_ID_EMPLOYEES)
            h_emp_df.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_EMPLOYEES, progress_bar= True, credentials=credentials, if_exists='append', table_schema= emp_schema)
            job_schema =  generate_bq_table_schema(TABLE_ID_JOBS)
            jobs_df.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_JOBS, progress_bar= True, credentials=credentials, if_exists='append', table_schema= job_schema)
            dep_schema = generate_bq_table_schema(TABLE_ID_DEPARTMENTS)
            dep_df.to_gbq(project_id=PROJECT_ID, destination_table=TABLE_ID_JOBS, progress_bar= True, credentials=credentials, if_exists='append', table_schema= dep_schema)
        else:
            return body_msg          
    else:    
        return body_msg

    return {"message":"Employees inserted."},200
    

   
    

if __name__ == "__main__":

    app.run(debug=True, port=5001) #local
    #app.run(debug=True, port=5001, '0.0.0.0') #cloud 
    