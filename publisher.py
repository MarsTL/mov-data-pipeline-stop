#!/usr/bin/env python3

from google.cloud import pubsub_v1
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from concurrent import futures
from google.oauth2 import service_account
from bs4 import BeautifulSoup

# Configuration
SERVICE_ACCOUNT_FILE = "/opt/shared/mov-data-pipeline-stop/service-account.json"
project_id = "mov-data-eng"
topic_id = "stop-events"

# Pub/Sub
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)

# Load vehicle IDs
with open("/opt/shared/vehicle_IDs.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]

# Get current date
now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")


# Publish Callback
def future_callback(future):
    try:
        future.result()
    except Exception as e:
        print(f"Error publishing message: {e}")

def parse(html_content):
    soup = BeautifulSoup(html_content, "lxml")
    tables = soup.find_all("table")
    all_records = []
    h2s = soup.find_all(name="h2", string=True)
    trip_ids = []
    for h2 in h2s:
      trip_ids.append(str(h2).strip("</h2>").split()[-1])
    
    trip_id_counter = 0
    for table in tables:
        if int(trip_ids[trip_id_counter]) <= 0:
          trip_id_counter += 1
          continue
        headers = [th.text.strip() for th in table.find_all("th")]
        headers.append("trip_id")
        first_row = table.find_all("tr")[1]
        cells = [td.text.strip() for td in first_row.find_all("td")]
        if cells:
            record = dict(zip(headers, cells))
            all_records.append(record)
        trip_id_counter += 1
    return all_records

count = 0
future_list = []

print(f"Publishing Stop Events data for {today}...")
'''
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
    
    #read json from file object 
    try:
        response = requests.get(url)
        content = response.text.strip()
        
        if response.status_code == 404:
            print(f"Failed to fetch data for {vehicle_id}")
            continue 
        
        else:
            #records = response.json()
            records = parse(content)
        if not records:
            print(f"No data found for vehicle{vehicle_id}")
            #publish pub/sub
            for record in records:
                message = json.dumps(record).encode("utf-8")
                #previously existed
                future = publisher.publish(topic_path, data=message)
                ### newly added lines
                future.add_done_callback(future_callback)
                future_list.append(future)
                count += 1
                if count % 50000 == 0:
                    print(f"Published {count} messages.")
                # for future in futures.as_completed(future_list):
                #    continue
            print(f"Published {len(records)} messages for vehicle {vehicle_id}")
            print(f"Wrote {len(records)} records for vehicle {vehicle_id}")
    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")
'''

for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"

    try:
        response = requests.get(url)
        content = response.text.strip()

        if response.status_code == 404 or not content:
            print(f"Failed to fetch data for {vehicle_id}")
            continue  
        
        #just to valivate  
        soup = BeautifulSoup(content, "lxml")
        tables = soup.find_all("table")
        if not tables:
            print(f"No data table found for vehicle {vehicle_id}")
            continue

        #parse HTML to JSON
        records = parse(content)

        if not records:
            print(f"No data found for vehicle {vehicle_id}")
            continue

        # Publish each record
        for record in records:
            message = json.dumps(record).encode("utf-8")
            future = publisher.publish(topic_path, data=message)
            future.add_done_callback(future_callback)
            future_list.append(future)
            count += 1

            if count % 50000 == 0:
                print(f"Published {count} messages.")

        print(f"Published {len(records)} messages for vehicle {vehicle_id}")
        print(f"Wrote {len(records)} records for vehicle {vehicle_id}")

    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")

for future in futures.as_completed(future_list):
        continue
            
print(f"Finished gathering breadcrumbs for {today}")
