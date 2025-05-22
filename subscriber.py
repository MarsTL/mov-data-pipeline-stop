from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import json
from datetime import datetime
from google.oauth2 import service_account
import pandas as pd
import psycopg2
import os
import io
import csv

#config
project_id = "mov-data-eng"
subscription_id = "stop-events-sub"  ##change
SERVICE_ACCOUNT_FILE = "/opt/shared/service-account.json"

#pub/sub setup
pubsub_creds2 = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds2)
subscription_path = subscriber.subscription_path(project_id, subscription_id)

#directory
OUTPUT_DIR = '/opt/shared/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)
def db_connect():
    return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="movv",
            host="localhost"
    )


json_data = []
TableName = 'trip'



def validate_vehicle_no(df):
  #validate it properly
  try:
    var = pd.to_numeric(df['vehicle_number'], errors='coerce')
    assert var.notna().all(), "vehicle_number contains null values"
    df['vehicle_number'] = var
    return df
  except Exception as e:
    print(f"Error validating column vehicle number: {e}")
    return None

def validate_route_number(df):
  try:
    var = pd.to_numeric(df['route_number'], errors='coerce')
    assert var.notna().all(), "route_number contains null values"
    df['route_number'] = var
    return df
  except Exception as e:
    print(f"Error validating column route number: {e}")
    return None

def validate_trip_number(df):
  try:
    var = pd.to_numeric(df['trip_number'], errors='coerce')
    assert var.notna().all(), "trip_number contains null values"
    df['trip_number'] = var
    return df
  except Exception as e:
    print(f"Error validating column trip number: {e}")
    return None

def validate_direction(df):
  try:
    assert df['direction'].notna().all(), "direction contains null values"
    df['direction'] = pd.to_numeric(df['direction'], errors='coerce')
    assert df['direction'].notna().all(), "direction contains null values"
    direct = [0,1]
    assert df['direction'].isin(direct).all(), "direction must be either 0 or 1"
    maps = {0: 'Out', 1: 'Back'}
    df['direction'] = df['direction'].map(maps)
    assert df['direction'].notna().all(), "direction contains null values"
    assert df['direction'].isin(['Out', 'Back']).all(), "direction must be either Out or Back"
    return df
  except Exception as e:
    print(f"Error validating column direction: {e}")
    return None

def validate_service_key(df):
  try:
    df['service_key'] = df['service_key'].astype(str).str.strip()
    df['service_key'] = df['service_key'].str.upper()
    keys = ['W', 'S', 'U']
    
    df['service_key'] = df['service_key'].apply(lambda x: x if x in keys else 'W')
    # assert df['service_key'].apply(lambda x: pd.isna(x) or x in keys).all(), "service_key must be either null, W, U, or S"
    maps = {'W': 'Weekday', 'S': 'Saturday', 'U': 'Sunday'}
    df['service_key'] = df['service_key'].replace(maps)
    assert df['service_key'].isin(['Weekday', 'Saturday', 'Sunday']).all(), "service_key must be either Weekday, Saturday, or Sunday"
    return df
  except Exception as e:
    print(f"Error validating column service key: {e}")
    return None

  
def validate_data(df):
  df = validate_vehicle_no(df)
  df = validate_route_number(df)
  df = validate_trip_number(df)
  df = validate_direction(df)
  df = validate_service_key(df)
  return df

#function to get existing trip ids from the trip table
def get_existing_trip_ids():
  conn = None
  try:
      conn = db_connect()
      with conn.cursor() as cursor:
          cursor.execute(f"SELECT trip_id FROM {TableName}")
          existing_trip_ids = [row[0] for row in cursor.fetchall()]
          return existing_trip_ids
  except Exception as e:
      print(f"Error getting existing trip ids: {e}")
      return []
  finally:
      if conn:
          conn.close()

#store both trip and everthing in the database
def store_database(df):
  conn = None
  #handle nan values for required columns
  try:
    df_new = df.copy()
    required_columns = ['trip_id', 'route_number', 'vehicle_number', 'service_key', 'direction']
    for col in required_columns:
      if col not in df_new.columns:
        print(f"Column {col} not found in DataFrame")
        return False
    #check for existing trip ids
    trip_ids = df_new['trip_id'].unique().tolist()
    print(f"trip ids: {len(trip_ids)} unique")

    df_new = df_new.rename(columns={'trip_id':'trip_id', 'route_number':'route_id', 'vehicle_number':'vehicle_id', 'service_key':'service_key', 'direction':'direction'})
    dataframe_data =  df_new[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]

    f = io.StringIO()
    dataframe_data.to_csv(f, header=False, index=False, sep ='\t')
    f.seek(0)
    conn = db_connect()
    with conn.cursor() as cursor:
        cursor.copy_from(f, TableName, sep='\t')
        conn.commit()
        print(f'stored {len(dataframe_data)} records in database')
        return True
  except Exception as e:
      print(f"error storing in database{e}")  
      if conn:  
          conn.rollback()
      return False
  finally:
      if conn:
        conn.close()

def callback(message):
  try:
     data = json.loads(message.data.decode('utf-8'))
     json_data.append(data)
     message.ack() 
  except Exception as e:
    print(f"Error processing message: {e}")
    message.nack()
 


def other_process(json_data):
  if not json_data:
    print("No data to process")
    return None

  try:
    df = pd.DataFrame(json_data)
    df = validate_data(df)
    print("data validation complete")

    if df is not None and not df.empty:
      save_db = store_database(df)
      if save_db:
        json_data = df.to_dict(orient='records')
        timestamp = datetime.now()
        filename = os.path.join(OUTPUT_DIR, timestamp.strftime('%Y-%m-%d') + '.json')
        try:
          with open(filename, 'a') as f:
            for data in json_data:
              json.dump(data, f)
              f.write('\n')
          print(f"Saved {len(json_data)} records to {filename}")
        except Exception as e:
          print(f"Error saving data to file: {e}")
        json_data.clear()
        return df
      else:
        print("Error storing data in database")
    else:
      print("No valid data to process")
    json_data.clear()
    return None
  except Exception as e:
    print(f"Error processing data: {e}")
    json_data.clear()
    return None


print(f"Listening for messages on {subscription_path}..\n")
while True:
  try:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result(timeout = 20)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print("Timeout occurred, exiting.")
    pro_df = other_process(json_data)
    if pro_df is not None:
      print(f"successfully processed  {len(pro_df)} records")
    else:
      print("no data to process")
  except KeyboardInterrupt:
    print('interrupted by keyboard')
    break
  except Exception as e:
    print(f"Error processing loop: {e}")
    continue
