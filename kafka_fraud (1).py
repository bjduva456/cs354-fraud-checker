from kafka import KafkaProducer
import json
import pandas as pd
import time

# Load your cleaned dataset
df = 

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send a single record
def send_transaction(record):
    producer.send('transactions', value=record)

# Stream transactions
for index, row in df.iterrows():
    transaction = {
        "trans_date_trans_time": row['trans_date_trans_time'],
        "cc_num": str(row['cc_num']),
        "merchant": row['merchant'],
        "category": row['category'],
        "amt": row['amt'],
        "first": row['first'],
        "last": row['last'],
        "gender": row['gender'],
        "street": row['street'],
        "city": row['city'],
        "state": row['state'],
        "zip": int(row['zip']),
        "lat": row['lat'],
        "long": row['long'],
        "city_pop": row['city_pop'],
        "job": row['job'],
        "dob": row['dob'],
        "trans_num": row['trans_num'],
        "unix_time": row['unix_time'],
        "merch_lat": row['merch_lat'],
        "merch_long": row['merch_long'],
        "is_fraud": row['is_fraud'], 
        "merch_zipcode": row['merch_zipcode'],
        "home_merchant_dist": row['home_merchant_dist'],
        "trans_timestamp": row['trans_timestamp'],
        "transaction_hour": row['transaction_hour'],
        "transaction_dayofweek": row['transaction_dayofweek'],
        "transaction_month": row['transaction_month']
    }
    
    send_transaction(transaction)
    
    print(f"Sent transaction {row['trans_num']}")
    
    time.sleep(0.5)  # Sleep to simulate real-time streaming
