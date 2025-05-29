import json
from datetime import datetime
import pandas as pd
import psycopg2
import os
import io
import csv
import shutil
import glob
from dotenv import load_dotenv 
load_dotenv()


#directory
INPUT_DIR = '/opt/shared/mov-data-pipeline-stop/bus_data/2025-05-28'
OUTPUT_DIR = '/opt/shared/mov-data-pipeline-stop/output2/2025-05-28'
os.makedirs(OUTPUT_DIR, exist_ok=True)
def db_connect():
    return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
    )


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
    
    maps = {'W': 'Weekday', 'S': 'Saturday', 'U': 'Sunday'}
    df['service_key'] = df['service_key'].replace(maps)
    assert df['service_key'].isin(['Weekday', 'Saturday', 'Sunday']).all(), "service_key must be either Weekday, Saturday, or Sunday"
    return df
  except Exception as e:
    print(f"Error validating column service key: {e}")
    return None

  
def validate_trip_id(df):
  try:
    var = pd.to_numeric(df['trip_id'], errors='coerce')
    assert var.notna().all(), "trip_id contains null values"
    df['trip_id'] = var
    return df
  except Exception as e:
    print(f"Error validating column trip id: {e}")
    return None

def validate_ons(df):
  try:
    var = pd.to_numeric(df['ons'], errors='coerce')
    assert var.notna().all(), "ons contains null values"
    df['ons'] = var
    return df
  except Exception as e:
    print(f"Error validating column ons: {e}")
    return None

def validate_offs(df):
  try:
    var = pd.to_numeric(df['offs'], errors='coerce')
    assert var.notna().all(), "offs contains null values"
    df['offs'] = var
    return df
  except Exception as e:
    print(f"Error validating column offs: {e}")
    return None
def validate_train(df):
  try:
    var = pd.to_numeric(df['train'], errors='coerce')
    assert var.notna().all(), "train contains null values"
    df['train'] = var
    return df    
  except Exception as e:
    print(f"Error validating column train: {e}")
    return None

def validate_max_speed(df):
  try:
    var = pd.to_numeric(df['maximum_speed'], errors='coerce')
    assert var.notna().all(), "maximum_speed contains null values"
    df['maximum_speed'] = var
    return df
  except Exception as e:
    print(f"Error validating column maximum_speed: {e}")
    return None

def validate_data(df):
  df = validate_vehicle_no(df)
  df = validate_route_number(df)
  df = validate_trip_number(df)
  df = validate_direction(df)
  df = validate_service_key(df)
  df = validate_trip_id(df)
  df = validate_ons(df)
  df = validate_offs(df)
  df = validate_train(df)
  df = validate_max_speed(df)
  return df




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

    df_new = df_new.rename(columns={'trip_id':'trip_id', 'route_number':'route_id', 'vehicle_number':'vehicle_id', 'service_key':'service_key', 'direction':'direction'})
    dataframe_data =  df_new[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']]
    #delete duplicate trip id rows
    dataframe_data = dataframe_data.drop_duplicates(subset=['trip_id'])
    

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


def other_process(file):
  try:
    json_data = []
    with open(file, 'r', encoding='utf-8') as f:
      json_data = json.load(f)
      print(f"loaded {len(json_data)} records from {file}")
      if not json_data:
        print(f"No data to process")
        return False
    df = pd.DataFrame(json_data)
    #remove duplicate trip id
    df = df.drop_duplicates(subset=['trip_id'])
    #validate data
    df = validate_data(df)
    print("data validation complete")

    if df is not None and not df.empty:
      save_db = store_database(df)
      if save_db:
        path = os.path.join(OUTPUT_DIR, os.path.basename(file))
        shutil.copy(file, path)
        print(f"copied {file} to {path}")
        return True
      else:
        print("Error storing data in database")
        return False
    else:
      print("No valid data to process")
      return False
  except Exception as e:
    print(f"Error processing data: {e}")
    return False


def main():
  gz = glob.glob(os.path.join(INPUT_DIR, '*.json'))
  if not gz:
    print("No files found")
    return 
  print(f"found {len(gz)} files")
  for file in gz:
    print(f"processing {file}")
    other_process(file)
  print("done")

if __name__ == '__main__':
  main()
