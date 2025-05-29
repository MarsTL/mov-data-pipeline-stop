import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

now = datetime.now(ZoneInfo("America/Los_Angeles"))
today = now.strftime("%Y-%m-%d")
directory = f'/opt/shared/mov-data-pipeline-stop/bus_data/2025-05-28'
count = 0
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        with open(os.path.join(directory, filename)) as f:
            try:
                data = json.load(f)
                count += len(data)
            except json.decoder.JSONDecodeError:
                print(f"Error decoding {filename}")
print(count)
