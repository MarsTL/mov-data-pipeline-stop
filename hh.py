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
from dotenv import load_dotenv 
load_dotenv()
#from pathlib import Path
#load_dotenv(dotenv_path=Path("/opt/shared/mov-data-pipeline-stop/.env"))
#print(os.getenv("DB_HOST")

class SubscriberTrip:
  def __init__(self):
    self.project_id = "mov-data-eng"
    self.subscription_id = "stop-events-sub"  ##change
    self.SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
    self.pubsub_creds2 = service_account.Credentials.from_service_account_file(self.SERVICE_ACCOUNT_FILE)
    self.subscriber = pubsub_v1.SubscriberClient(credentials=self.pubsub_creds2)
    self.subscription_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)
    self.json_data = []
    self.TableName = 'trip'
    self.OUTPUT_DIR = '/opt/shared/mov-data-pipeline-stop/output'
    os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
  def db_connect(self):
    return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST")
    )
  



  def validate_vehicle_no(self,df):
    #validate it properly
    try:
      var = pd.to_numeric(df['vehicle_number'], errors='coerce')
      assert var.notna().all(), "vehicle_number contains null values"
      df['vehicle_number'] = var
      return df
    except Exception as e:
      print(f"Error validating column vehicle number: {e}")
      return None

  def validate_route_number(self,df):
    try:
      var = pd.to_numeric(df['route_number'], errors='coerce')
      assert var.notna().all(), "route_number contains null values"
      df['route_number'] = var
      return df
    except Exception as e:
      print(f"Error validating column route number: {e}")
      return None

  def validate_trip_number(self,df):
    try:
      var = pd.to_numeric(df['trip_number'], errors='coerce')
      assert var.notna().all(), "trip_number contains null values"
      df['trip_number'] = var
      return df
    except Exception as e:
      print(f"Error validating column trip number: {e}")
      return None

  def validate_direction(self,df):
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

  def validate_service_key(self,df):
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

  def validate_trip_id(self,df):
    try:
      var = pd.to_numeric(df['trip_id'], errors='coerce')
      assert var.notna().all(), "trip_id contains null values"
      df['trip_id'] = var
      return df
    except Exception as e:
      print(f"Error validating column trip id: {e}")
      return None

  def validate_ons(self,df):
    try:
      var = pd.to_numeric(df['ons'], errors='coerce')
      assert var.notna().all(), "ons contains null values"
      df['ons'] = var
      return df
    except Exception as e:
      print(f"Error validating column ons: {e}")
      return None

  def validate_offs(self,df):
    try:
      var = pd.to_numeric(df['offs'], errors='coerce')
      assert var.notna().all(), "offs contains null values"
      df['offs'] = var
      return df
    except Exception as e:
      print(f"Error validating column offs: {e}")
      return None
  def validate_train(self,df):
    try:
      var = pd.to_numeric(df['train'], errors='coerce')
      assert var.notna().all(), "train contains null values"
      df['train'] = var
      return df    
    except Exception as e:
      print(f"Error validating column train: {e}")
      return None

  def validate_max_speed(self,df):
    try:
      var = pd.to_numeric(df['maximum_speed'], errors='coerce')
      assert var.notna().all(), "maximum_speed contains null values"
      df['maximum_speed'] = var
      return df
    except Exception as e:
      print(f"Error validating column maximum_speed: {e}")
      return None

  def validate_data(self,df):
    df = self.validate_vehicle_no(df)
    df = self.validate_route_number(df)
    df = self.validate_trip_number(df)
    df = self.validate_direction(df)
    df = self.validate_service_key(df)
    df = self.validate_trip_id(df)
    df = self.validate_ons(df)
    df = self.validate_offs(df)
    df = self.validate_train(df)
    df = self.validate_max_speed(df)
    return df

    
  
  #store both trip and everything in the database
  def store_database(self,df):
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
      conn = self.db_connect()
      with conn.cursor() as cursor:
          cursor.copy_from(f, self.TableName, sep='\t')
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

  def callback(self,message):
    try:
      data = json.loads(message.data.decode('utf-8'))
      self.json_data.append(data)
      message.ack() 
    except Exception as e:
      print(f"Error processing message: {e}")
      message.nack()
  


  def other_process(self,json_data):
    if not json_data:
      print("No data to process")
      return None

    try:
      df = pd.DataFrame(json_data)
      #remove duplicate trip id
      df = df.drop_duplicates(subset=['trip_id'])
      #validate data
      df = self.validate_data(df)
      print("data validation complete")

      if df is not None and not df.empty:
        save_db = self.store_database(df)
        if save_db:
          json_data = df.to_dict(orient='records')
          timestamp = datetime.now()
          filename = os.path.join(self.OUTPUT_DIR, timestamp.strftime('%Y-%m-%d') + '.json')
          try:
            with open(filename, 'a') as f:
              for data in json_data:
                json.dump(data, f)
                f.write('\n')
            print(f"Saved {len(json_data)} records to {filename}")
          except Exception as e:
            print(f"Error saving data to file: {e}")
          return df
        else:
          print("Error storing data in database")
      else:
        print("No valid data to process")
      return None
    except Exception as e:
      print(f"Error processing data: {e}")
      return None

  def run(self):
    print(f"Listening for messages on {self.subscription_path}..\n")
    try:   
      while True:
        try:
          streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
          streaming_pull_future.result(timeout=20)
        except TimeoutError:
          print("Timeout occurred")
        except Exception as e:
          print(f"subscription error: {e}")
        finally:
          try:
            streaming_pull_future.cancel()
            streaming_pull_future.result(timeout =3)
          except:
            pass
        pro_df = self.other_process(self.json_data)
        if pro_df is not None:
          print(f"successfully processed  {len(pro_df)} records")
        else:
          print("no data to process")
        self.json_data.clear()
    except KeyboardInterrupt:
      print('interrupted by keyboard')
      print('process remaining data')
      self.other_process(self.json_data)

    except Exception as e:
      print(f"Error processing loop: {e}")

if __name__ == '__main__':
  subscriber = SubscriberTrip()
  subscriber.run()
          

