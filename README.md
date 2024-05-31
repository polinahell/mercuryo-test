Mercuryo Take-Home Assessment

This Python script aggregates taxi trip data from a CSV file based on the pickup date and payment type, and inserts the results into a PostgreSQL database. 


## Prerequisites
- Python 3._
- PostgreSQL
- Pandas library
- psycopg2 library

Python libraries can be installed via pip:

    pip install pandas psycopg2

## Database Setup
Create a PostgreSQL database.
Within this database, create a table for the aggregated results using the following SQL command:

    CREATE TABLE public.nyc_taxi_aggregated (
        day TIMESTAMP,
        payment_type VARCHAR(3),
        rides INTEGER,
        amount DOUBLE PRECISION,
        PRIMARY KEY (day, payment_type)
    );

### Configuration
Before running the script, ensure the PostgreSQL database credentials and connection details in the script match the environment settings. 

# Running the Script
Place your CSV data file in the appropriate directory into data folder.

Place your SQL query in a query.sql [ file.](query.sql)

## Execution
The script by navigating to the script's directory and running:

    python main.py 2015-01-01

Replace 2015-01-01 with the specific date you want to process data for.
