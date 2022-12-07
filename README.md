the python script is develop to enable exporting of google sheet to postgres databse based host in a docker contain
readgsheet.py contains python code which enable secure access to googlesheet using a service-account
and establishes connection to the postgres database using psycopg2 and sqlalchemy.
Airflow.py contains python script to schedule jobs to automatic process of exporting
googlesheet to postgres database. 