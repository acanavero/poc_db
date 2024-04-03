import pandas as pd
from pandas.errors import ParserError

def validate_columns(emps,deps,jobs):
    
    """Recieves the dataframes and checks that the columns are correct. Returns a message and a code"""
    
    if not(set(emps.columns) == set(["id",'name','datetime','department_id','job_id'])):
        return {"error": "Invalid employees columns"}, 400 
    elif not(set(jobs.columns) == set(["id",'job'])):
        return {"error": "Invalid jobs columns"}, 400 
    elif not(set(deps.columns) == set(["id",'department'])):
        return {"error": "Invalid department columns"}, 400 
    
    return {"message":"columns are correct!"},200

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
    """Receives 3 dataframes and cast all the columns into the correct type. 
    Also verifies if all the datetime values are valid and. 
    Also verifies if any df has more then 1000 rows. 
    Returns body msg and a code"""
    try:
        if emp.shape[1] < 1000:
            emp['id'] = emp['id'].astype('int')
            emp['department_id'] = emp['job_id'].astype('int')
            emp['datetime'] = emp['datetime'].astype('string')
            try:
                pd.to_datetime(emp['datetime']) #check if df has valid dates 
            except (ParserError,ValueError): 
                return emp,dep,job, {"error": "Your employees df has invalid dates"}, 400
            
            emp['name'] = emp['name'].astype('string')
            emp['job_id'] = emp['job_id'].astype('int')
        
            print(emp.info())
        else:
            print(emp.shape[1])
            return emp, dep, job, {"error": "employees should have only up to 1000 rows"}, 400
        
        if dep.shape[1] < 1000:
            dep['id'] = dep['id'].astype('int')
            dep["department"] = dep["department"].astype('string')
        else:
            return emp,dep,job, {"error": "departments should have only up to 1000 rows"}, 400
        
        if job.shape[1] < 1000:
            job['id'] = job['id'].astype('int')
            job["job"] = job["job"].astype('string')
        else:
            return emp,dep,job,{"error": "jobs should have only up to 1000 rows"}, 400
        
        print(job.info())
        print(dep.info())
    
        return emp,dep,job,{"message": "Datatypes parsed succesfully"}, 200
    except Exception as e:
        print(e)
        return emp,dep,job,{"error": e}, 400
