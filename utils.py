from google.cloud import bigquery

PROJECT_ID = 'hallowed-valve-370617'

TABLE_ID_EMPLOYEES = 'hallowed-valve-370617.db_employees.hired_employees'

TABLE_ID_DEPARTMENTS = 'hallowed-valve-370617.db_employees.departments'

TABLE_ID_JOBS = 'hallowed-valve-370617.db_employees.jobs'

CREDENTIALS_PATH = 'adriano-test.json'

hired_employees_schema =[{'name': 'id', 'type': 'INTEGER', 'mode':'REQUIRED'}, 
                         {"name": 'name', 'type': 'STRING', 'mode':'REQUIRED'}, 
                         {'name': 'datetime', 'type': 'STRING', 'mode':'REQUIRED'}, 
                         {'name': 'department_id', 'type': 'INTEGER', 'mode':'REQUIRED'}, 
                         {'name': 'job_id', 'type': 'INTEGER', 'mode':'REQUIRED'},
                         ]

# Define the schema for departments
departments_schema = [{'name': 'id', 'type': 'INTEGER', 'mode':'REQUIRED'}, 
                         {'name': 'department', 'type': 'STRING', 'mode':'REQUIRED'}, 
                         ]
# Define the schema for jobs
jobs_schema = [{'name': 'id', 'type': 'INTEGER', 'mode':'REQUIRED'}, 
                {'name': 'job', 'type': 'STRING', 'mode':'REQUIRED'}, 
                         ]