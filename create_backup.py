from google.oauth2 import service_account
from fastavro import writer, parse_schema
from utils import employees_schema, execute_query, CREDENTIALS_PATH, PROJECT_ID
import pandas as pd
from datetime import date
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

PATH_E = f"./backup/avro_files/hired_employees_{str(date.today())}.avro"
PATH_D = f"./backup/avro_files/departments_{str(date.today())}.avro"
PATH_J = f"./backup/avro_files/jobs_{str(date.today())}.avro"

def backup_employees():

    """Saves table backup in avro format"""

    avro_employee_schema = {
                            "type" : "record",
                            "namespace" : "db_employees",
                            "name" : "hired_employees",
                            "fields" : [
                                { "name" : "id" , "type" : "int" },
                                { "name" : "name" , "type" : "string" },
                                { "name" : "datetime" , "type" : "string" },
                                { "name" : "department_id" , "type" : "int" },
                                { "name" : "job_id" , "type" : "int" }
                                ]
                            }

    query = """SELECT * FROM `hallowed-valve-370617.db_employees.hired_employees`"""

    df_employees = pd.read_gbq(query=query, project_id=PROJECT_ID, credentials=credentials).to_dict('records') #PASAR A RECORDS ANTES DE AVRO

    print(df_employees)

    with open(PATH_E, 'wb') as file:
        writer(file,avro_employee_schema,df_employees)

def backup_jobs():
    """Saves table backup in avro format"""

    avro_job_schema = {
                            "type" : "record",
                            "namespace" : "db_employees",
                            "name" : "jobs",
                            "fields" : [
                                { "name" : "id" , "type" : "int" },
                                { "name" : "job" , "type" : "string" },
                                ]
                            }

    query = """SELECT * FROM `hallowed-valve-370617.db_employees.jobs`"""

    df = pd.read_gbq(query=query, project_id=PROJECT_ID, credentials=credentials).to_dict('records') #PASAR A RECORDS ANTES DE AVRO

    print(df)

    with open(PATH_J, 'wb') as file:
        writer(file,avro_job_schema,df)

def backup_departments():

    """Saves table backup in avro format"""

    avro_department_schema = {
                            "type" : "record",
                            "namespace" : "db_employees",
                            "name" : "departments",
                            "fields" : [
                                { "name" : "id" , "type" : "int" },
                                { "name" : "department" , "type" : "string" },
                                ]
                            }

    query = """SELECT * FROM `hallowed-valve-370617.db_employees.departments`"""

    df = pd.read_gbq(query=query, project_id=PROJECT_ID, credentials=credentials).to_dict('records') #cast to records before avro

    print(df)

    with open(PATH_D, 'wb') as file:
        writer(file,avro_department_schema,df)

    print("Department backup succesful!")
    
if __name__ == "__main__":

    #backup_employees()
    #backup_jobs()
    backup_departments()
    