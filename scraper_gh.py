import requests
from datetime import datetime, timedelta
from db_connection import create_connection, execute_query
import time
import json
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


class DiscomDrawalScheduleScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'https://uksldc.com/ViewReportSchedule/Index/GetNetSchedule'
        })

        # Database Connection
        self.connection = create_connection(
            db_name='your_database_name',
            db_user='your_db_username',
            db_password='your_db_password',
            db_host='your_db_host',
            db_port='your_db_port'
        )
        if self.connection:
            print("✔ Database connection established")
            self._create_table()
        else:
            print("✖ Database connection failed")

    def _create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS discom_drawal_schedule (
            id SERIAL PRIMARY KEY,
            schedule_date DATE NOT NULL,
            discom_name VARCHAR(100) NOT NULL,
            time_block INT NOT NULL,
            time_range VARCHAR(50),
            scheduled_drawal FLOAT,
            actual_drawal FLOAT,
            deviation FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_discom_date ON discom_drawal_schedule (discom_name, schedule_date);
        """
        execute_query(self.connection, query)

    def download_daily_data(self, date):
        date_str = date.strftime('%m/%d/%Y')
        url = 'https://uksldc.com/ViewReportSchedule/GetDiscomData'

        for sldcrevision in range(9, -1, -1):
            payload = {
                'fromDate': date_str,
                'sldcrevision': str(sldcrevision),
                'formate': 'M/d/yyyy',
                'type': '5',
                'entityid': '-1',
                'customertype': '3'
            }

            try:
                response = self.session.post(url, data=payload)
                if response.status_code == 200:
                    try:
                        # First ensure we have proper JSON
                        data = response.json()
                        if not isinstance(data, list):
                            print("Response is not a list - attempting to fix")
                            try:
                                data = json.loads(response.text)
                            except:
                                data = response.json()

                        print(f"Successfully got data for {date_str}")
                        return data
                    except ValueError as e:
                        print(f"JSON parse error: {str(e)}")
                        print(f"Response content: {response.text[:500]}...")
                elif response.status_code == 500:
                    continue
            except Exception as e:
                print(f"Request failed: {str(e)}")

        print(f"Failed to get valid data for {date_str}")
        return None

    def process_data(self, json_data, date):
        processed_data = []

        if not json_data:
            print("No data to process")
            return processed_data

        # Handle case where we might get a string instead of parsed JSON
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON string: {str(e)}")
                return processed_data

        if not isinstance(json_data, list):
            print(f"Unexpected data format: {type(json_data)}")
            return processed_data

        print(f"Processing {len(json_data)} records for {date}")

        for record in json_data:
            try:
                if not isinstance(record, dict):
                    continue

                # Skip summary rows that contain text values
                time_block = record.get('TimeBlock', '0')
                if not time_block.isdigit():
                    continue

                time_range = record.get('TimeDesc', '')

                # Get all discom names (exclude special columns)
                discom_names = [
                    key for key in record.keys()
                    if key not in ['TimeBlock', 'TimeDesc', 'Total', 'ISGS']
                       and not key.startswith('_')
                ]

                for discom in discom_names:
                    try:
                        value = record.get(discom, '0.00')
                        # Skip if the value is non-numeric (like "Average", "MWH", etc.)
                        if isinstance(value, str) and not value.replace('.', '').isdigit():
                            continue

                        scheduled_drawal = float(value) if value else 0.0

                        processed_data.append({
                            'schedule_date': date,
                            'discom_name': discom,
                            'time_block': int(time_block),
                            'time_range': time_range,
                            'scheduled_drawal': scheduled_drawal,
                            'actual_drawal': None,
                            'deviation': None
                        })
                    except (ValueError, TypeError) as e:
                        print(f"Skipping non-numeric value for {discom}: {value}")
                        continue
            except Exception as e:
                print(f"Error processing record: {str(e)}")
                print(f"Problematic record: {record}")
                continue

        print(f"Successfully processed {len(processed_data)} valid records")
        return processed_data

    def save_to_database(self, data):
        if not data:
            return False

        query = """
        INSERT INTO discom_drawal_schedule 
        (schedule_date, discom_name, time_block, time_range, scheduled_drawal, actual_drawal, deviation)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (schedule_date, discom_name, time_block) 
        DO UPDATE SET
            scheduled_drawal = EXCLUDED.scheduled_drawal,
            time_range = EXCLUDED.time_range,
            deviation = EXCLUDED.deviation
        """

        try:
            cursor = self.connection.cursor()
            batch = [
                (
                    d['schedule_date'], d['discom_name'], d['time_block'],
                    d['time_range'], d['scheduled_drawal'], d['actual_drawal'], d['deviation']
                )
                for d in data
            ]
            cursor.executemany(query, batch)
            self.connection.commit()
            print(f"Saved {len(data)} records")
            return True
        except Exception as e:
            print(f"Database error: {str(e)}")
            self.connection.rollback()
            return False

    def scrape_date_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            print(f"\nProcessing {current_date.strftime('%Y-%m-%d')}")

            data = self.download_daily_data(current_date)
            if not data:
                current_date += timedelta(days=1)
                continue

            processed = self.process_data(data, current_date)
            if not processed:
                current_date += timedelta(days=1)
                continue

            if not self.save_to_database(processed):
                current_date += timedelta(days=1)
                continue

            current_date += timedelta(days=1)

    def close(self):
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
        self.session.close()


if __name__ == "__main__":
    scraper = DiscomDrawalScheduleScraper()
    try:
        # Example date range - replace with your actual dates
        scraper.scrape_date_range(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31)  # Example one month range
        )
    finally:
        scraper.close()