                                          Proof of concept cloud database using BigQuery and AWS EC2.

The module "create_tables_bq.py" converts the CSV files into dataframes to upload them into BigQuery.
The table schemas are defined in the module "utils.py" which indicates that all fields are required.

The API is in the module "app.py" and it uses the module "data_validation.py" to check if the request body data sent by the client follows the data rules, such as data types, column names, valid datetime strings, and no null values. The "data_validation.py" module also validates that the transactions of each table are between 1 and 1000 rows.
The API receives data from three different tables in one transaction.

The API has an endpoint that returns a table in JSON format that provides the following information: "Number of employees hired for each job and department divided by quarter". The data is received using a pre-established view in BigQuery.

The module "create_backup.py" executes queries to BigQuery to get the actual state of the database and saves a backup in AVRO format. The resulting file name has the following format: "{table_name}_{yyyy-mm-dd}.avro".

Regarding security considerations, the instance in AWS EC2 only responds to predefined IP addresses. Also, the virtual instance is only accesible using a private key.

To run in local environment:

Install python 3.8.10. 

pip install flask

pip install pandas

pip install fastavro

pip install --upgrade google-cloud-bigquery

run python3 app.py 
