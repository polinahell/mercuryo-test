import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sys
from datetime import datetime, timedelta

def read_query_from_file(filename, date):
    with open(filename, 'r') as file:
        query = file.read()
    return query.replace('{{date}}', date)

# Function to get the previous day's date
def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Get the date from command-line arguments or use yesterday's date
if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    date = get_yesterday()

# Define the path to the CSV file and the SQL query file
csv_file_path = r'C:\Users\37259\Desktop\Programming\Mercuryo\mercuryo-test\data\trips.csv'
query_file_path = r'C:\Users\37259\Desktop\Programming\Mercuryo\mercuryo-test\query.sql'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Filter the DataFrame to only include rows where pickup_datetime is the specified date
df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
filtered_df = df[df['pickup_datetime'].dt.date == pd.to_datetime(date).date()].copy()

# Perform the aggregation
filtered_df['day'] = filtered_df['pickup_datetime'].dt.date
aggregated_df = filtered_df.groupby(['day', 'payment_type']).agg(
    rides=('trip_id', 'count'),
    amount=('total_amount', 'sum')
).reset_index()

# Ensure the 'payment_type' is a string
aggregated_df['payment_type'] = aggregated_df['payment_type'].astype(str)

# Convert the 'day' column to datetime for PostgreSQL compatibility
aggregated_df['day'] = pd.to_datetime(aggregated_df['day'])

# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname="nyc_taxi_data",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Function to check if a row exists
def row_exists(day, payment_type):
    query = "SELECT 1 FROM nyc_taxi_data WHERE day = %s AND payment_type = %s"
    cursor.execute(query, (day, payment_type))
    return cursor.fetchone() is not None

# Insert or update data in PostgreSQL table
for index, row in aggregated_df.iterrows():
    day, payment_type, rides, amount = row
    payment_type = str(payment_type)  # Ensure payment_type is a string
    if row_exists(day, payment_type):
        update_query = """
        UPDATE nyc_taxi_data
        SET rides = %s, amount = %s
        WHERE day = %s AND payment_type = %s
        """
        cursor.execute(update_query, (rides, amount, day, payment_type))
    else:
        insert_query = """
        INSERT INTO nyc_taxi_data (day, payment_type, rides, amount)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (day, payment_type, rides, amount))

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Aggregated data has been successfully inserted/updated into the PostgreSQL table.")
