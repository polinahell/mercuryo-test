import pandas as pd
import psycopg2
import sys
from datetime import datetime, timedelta

def read_query_from_file(filename, date):
    with open(filename, 'r') as file:
        query = file.read()
    return query.replace('{{date}}', date)

def get_yesterday():
    return (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    date = get_yesterday()

query_file_path = r'C:\Users\37259\Desktop\Programming\Mercuryo\mercuryo-test\query.sql'
query = read_query_from_file(query_file_path, date)

csv_file_path = r'C:\Users\37259\Desktop\Programming\Mercuryo\mercuryo-test\data\trips.csv'

df = pd.read_csv(csv_file_path)

df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')

filtered_df = df[df['pickup_datetime'].dt.date == pd.to_datetime(date).date()].copy()

print("Filtered data")
print(filtered_df)

filtered_df['day'] = filtered_df['pickup_datetime'].dt.floor('d')
aggregated_df = filtered_df.groupby(['day', 'payment_type']).agg(
    rides=('trip_id', 'count'),
    amount=('total_amount', 'sum')
).reset_index()

aggregated_df['payment_type'] = aggregated_df['payment_type'].astype(str)
aggregated_df['day'] = pd.to_datetime(aggregated_df['day'])

print("Aggregated data")
print(aggregated_df)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="nyc_taxi_data",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

def row_exists(day, payment_type):
    query = "SELECT 1 FROM public.nyc_taxi_aggregated WHERE day = %s AND payment_type = %s"
    cursor.execute(query, (day, payment_type))
    return cursor.fetchone() is not None

for index, row in aggregated_df.iterrows():
    day, payment_type, rides, amount = row
    payment_type = str(payment_type)
    day = pd.Timestamp(day).to_pydatetime()
    try:
        amount = float(amount)
    except ValueError as e:
        print(f"Error converting amount to float")
        continue

    if row_exists(day, payment_type):
        update_query = """
        UPDATE public.nyc_taxi_aggregated
        SET rides = %s, amount = %s
        WHERE day = %s AND payment_type = %s
        """
        cursor.execute(update_query, (rides, amount, day, payment_type))
    else:
        insert_query = """
        INSERT INTO public.nyc_taxi_aggregated (day, payment_type, rides, amount)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (day, payment_type, rides, amount))

conn.commit()
cursor.close()
conn.close()

print("Aggregated data has been successfully into the PostgreSQL")
