import pandas as pd
from pandas.errors import ParserError

from logger import Logger


def validate_columns(emps,deps,jobs):
    
    """Recieves a df and a type and returns body message"""
    
    if not(set(emps.columns) == set(["id",'name','datetime','department_id','job_id'])):
        return {"error": "Invalid employees columns"}, 403 
    elif not(set(deps.columns) == set(["id",'job'])):
        return {"error": "Invalid departments columns"}, 403 
    elif not(set(jobs.columns) == set(["id",'department'])):
        return {"error": "Invalid jobs columns"}, 403 
    
    return {"message":"all tables were updated"},200

def verify_null(h,d,j):
    """Returns true if df has no nulls"""
    args = [h,d,j]
    for df in args:
        has_not_nulls = not df.isnull().values.any()
        if not has_not_nulls:
            continue  
        else:
            return  False
    return True

def parse_all_types(emp,dep,job):
    if emp.shape[1] < 1000:
        emp['id'] = emp['id'].astype('int')
        emp['department_id'] = emp['job_id'].astype('int')
        emp['datetime'] = emp['datetime'].astype('string')
        try:
            pd.to_datetime(emp['datetime']) #check if df has valid dates 
        except (ParserError,ValueError): 
            Logger.exception()
            return {"error": "Your employees df has invalid dates"}, 400
        
        emp['name'] = emp['name'].astype('string')
        emp['job_id'] = emp['job_id'].astype('int')
    
        print(emp.info())
    else:
        print(emp.shape[1])
        return {"error": "employees should have only up to 1000 rows"}, 400
    
    if dep.shape[1] > 100:
        return {"error": "departments should have only up to 1000 rows"}, 400
    
    if job.shape[1] > 100:
        return {"error": "jobs should have only up to 1000 rows"}, 400
    
    return True