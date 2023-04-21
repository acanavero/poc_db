from google.cloud import bigquery
from google.oauth2 import service_account
from utils import CREDENTIALS_PATH, PROJECT_ID

def create_connection():
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

    client = bigquery.Client(credentials = credentials, project = PROJECT_ID)

    return client

def execute_query(client, query):

    """ Ejecuta las queries y obtiene los resultados de BQ. """

    query_statement = client.query(query)

    return query_statement.result()

def generate_bq_table_schema(table_id):

    client = create_connection()
    
    table = client.get_table(table_id)

    generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]

    return  generated_schema
