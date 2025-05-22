import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# Load vehicle IDs
with open("/opt/shared/mov-data-pipeline-stop/vehicle_IDs.txt", "r") as f:
    vehicle_ids = [line.strip() for line in f if line.strip()]


# Get current time
now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")

# Create output directory for the day
output_dir = f"/opt/shared/mov-data-pipeline-stop/bus_data/{today}"
os.makedirs(output_dir, exist_ok=True)

# Iterate through each vehicle ID
for vehicle_id in vehicle_ids:
    url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"
    filename = os.path.join(output_dir, f"{vehicle_id}.json")

    try:
        response = requests.get(url)
        content = response.text.strip()

        if response.status_code != 200:
            print(f"Failed to fetch data for {vehicle_id}")
            continue

        # Parse HTML
        soup = BeautifulSoup(content, "lxml")
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
                record["trip_id"] = trip_ids[trip_id_counter]
                all_records.append(record)
            trip_id_counter += 1

        # Write to file if data is found
        if all_records:
            with open(filename, "w") as outfile:
                json.dump(all_records, outfile, indent=2)
            print(f"Wrote {len(all_records)} records for vehicle {vehicle_id}")
        else:
            print(f"No data found for vehicle {vehicle_id}, file not created")

    except Exception as e:
        print(f"Error for {vehicle_id}: {e}")

print(f"Finished gathering breadcrumbs for {today}")
print(f"{len(vehicle_ids)}")
