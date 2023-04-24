from datetime import datetime

def log(msg,code,data):
    try:
        with open(f'./logs/log{datetime.now().day}-{datetime.now().month}-{datetime.now().year}.txt', "a") as f:
            f.write(f"{str(datetime.now())}\n")
            f.write(f"{str(msg)} - {str(code)}\n")
            f.write(f"{str(data)}\n")
            f.close()
    except:
        with open(f'./logs/error{datetime.now().day}-{datetime.now().month}-{datetime.now().year}.txt', "a") as f:  
            f.write("there was an error logging unsuccessful transaction")
            f.close()