from google.cloud import pubsub_v1
import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from concurrent import futures
from google.oauth2 import service_account
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

class StopEventPublisher:
    def __init__(self, service_account_file, project_id, topic_id, vehicle_id_file):
        self.service_account_file = service_account_file
        self.project_id = project_id
        self.topic_id = topic_id
        self.vehicle_id_file = vehicle_id_file
        self.publisher = self._init_publisher()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.vehicle_ids = self._load_vehicle_ids()
        self.today = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")
        self.count = 0
        self.future_list = []

    def _init_publisher(self):
        credentials = service_account.Credentials.from_service_account_file(self.service_account_file)
        return pubsub_v1.PublisherClient(credentials=credentials)

    def _load_vehicle_ids(self):
        with open(self.vehicle_id_file, "r") as f:
            return [line.strip() for line in f if line.strip()]

    def _future_callback(self, future):
        try:
            future.result()
        except Exception as e:
            print(f"Error publishing message: {e}")

    def _parse(self, html_content):
        soup = BeautifulSoup(html_content, "lxml")
        tables = soup.find_all("table")
        h2s = soup.find_all("h2")
        trip_ids = [str(h2).strip("</h2>").split()[-1] for h2 in h2s]

        all_records = []
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

        return all_records

    def publish(self):
        print(f"Publishing Stop Events data for {self.today}...")

        for vehicle_id in self.vehicle_ids:
            url = f"https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_id}"

            try:
                response = requests.get(url)
                content = response.text.strip()

                if response.status_code == 404 or not content:
                    print(f"Failed to fetch data for {vehicle_id}")
                    continue

                soup = BeautifulSoup(content, "lxml")
                if not soup.find_all("table"):
                    print(f"No data table found for vehicle {vehicle_id}")
                    continue

                records = self._parse(content)

                if not records:
                    print(f"No data found for vehicle {vehicle_id}")
                    continue

                for record in records:
                    message = json.dumps(record).encode("utf-8")
                    future = self.publisher.publish(self.topic_path, data=message)
                    future.add_done_callback(self._future_callback)
                    self.future_list.append(future)
                    self.count += 1

                    if self.count % 50000 == 0:
                        print(f"Published {self.count} messages.")

                print(f"Published {len(records)} messages for vehicle {vehicle_id}")
                print(f"Wrote {len(records)} records for vehicle {vehicle_id}")

            except Exception as e:
                print(f"Error for {vehicle_id}: {e}")

        for future in futures.as_completed(self.future_list):
            continue

        print(f"Finished gathering stop event for {self.today}")


if __name__ == "__main__":
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
    VEHICLE_ID_FILE = "/opt/shared/mov-data-pipeline-stop/vehicle_IDs.txt"

    publisher = StopEventPublisher(
        service_account_file=SERVICE_ACCOUNT_FILE,
        project_id="mov-data-eng",
        topic_id="stop-events",
        vehicle_id_file=VEHICLE_ID_FILE
    )
    publisher.publish()

