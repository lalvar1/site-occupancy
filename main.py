import logging
from datetime import datetime
import os
import json
import uuid
import urllib3
import sys
from math import ceil
from dotenv import load_dotenv
from cloud.big_query import BigQueryProcessor
from local_db.local_db import SQLite
from users.Airtable import Airtable

SITE = 'bcn'
if SITE == 'bcn':
    import UDM_PRO.unify as controller
else:
    import UDM.unify as controller

script_path = os.path.dirname(os.path.realpath(__file__))
load_dotenv(f'{script_path}/.env')
urllib3.disable_warnings()
UNIFI_USER = os.getenv('USERNAME')
UNIFI_PWD = os.getenv('PASSWORD')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
DEST_PROJECT_ID = 'fenix-occupancy-data-server'
DEST_DATASET_ID = 'site_occupancy'
DEST_TABLE_ID = SITE
DEST_TABLE_SCHEMA = {'name': 'user', 'type': 'STRING', 'mode': 'REQUIRED'}, \
                    {'name': 'spent', 'type': 'INTEGER', 'mode': 'NULLABLE'}, \
                    {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'}, \
                    {'name': 'uuid', 'type': 'STRING', 'mode': 'REQUIRED'}
SVC_ACCOUNT = f'{script_path}/files/svc_account_occupancy.json'
DEST_JSON_FILE = f'{script_path}/files/bq_user_updates.json'
DAILY_STATS_FILE = f'{script_path}/files/daily_stats.json'

root = logging.getLogger()
root.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(filename='automation.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
handler.setFormatter(formatter)
root.addHandler(handler)


class OccupancyServer:
    """
    Periodically queries users occupancy from
    Unify Controller and updates daily user metrics
    """

    def __init__(self):
        self.daily_stats_file = DAILY_STATS_FILE
        self.current_stats = self.get_users_file_data()
        self.unifi_stats = {}

    def get_users_file_data(self):
        """Read current daily stats from file"""
        f = open(self.daily_stats_file)
        data = json.load(f)
        return data

    def load_status_file(self):
        """Updates local file based on current and unifi stats"""
        # logging.info("Updating daily stats data...")
        for user, unifi_data in self.unifi_stats.items():
            if user in self.current_stats:
                for mac, time in unifi_data.items():
                    self.current_stats[user][mac] = time
            else:
                self.current_stats[user] = unifi_data
        with open(self.daily_stats_file, 'w') as f:
            f.write(json.dumps(self.current_stats, indent=4))
        f.close()
        # logging.info("JSON file updated")

    def join_user_data(self):
        """Join MACs data from users over different APs"""
        for user, data in self.current_stats.items():
            self.current_stats[user] = {
                "spent": sum(data.values()),
                "date": datetime.today().strftime('%Y-%m-%d')
            }
        with open(self.daily_stats_file, 'w') as f:
            f.write(json.dumps(self.current_stats, indent=4))
        f.close()

    def update_site_users(self):
        """Clean up user list for Airtable table users"""
        airtable = Airtable(AIRTABLE_API_KEY, AIRTABLE_TABLE_NAME, AIRTABLE_BASE_ID)
        airtable_users = airtable.get_users()
        user_stats = {}
        online_hostnames = list(self.current_stats.keys())
        for airtable_user, data in airtable_users.items():
            if data['location'] != SITE:
                continue
            if airtable_user in online_hostnames:
                user = {data['full_name']: self.current_stats[airtable_user]}
            else:
                user = {
                    data['full_name']:
                        {
                            "spent": 0,
                            "date": datetime.today().strftime('%Y-%m-%d')
                        }
                    }
            user_stats.update(user)
        self.current_stats = user_stats

    def restart_daily_stats(self):
        with open(self.daily_stats_file, 'w') as f:
            f.write(json.dumps({}, indent=4))
        f.close()

    def get_sql_rows(self):
        """Get formatted SQL rows"""
        rows = [(key, *list(values.values()), str(uuid.uuid4())) for key, values in self.current_stats.items()]
        return rows

    def get_biq_query_rows(self):
        """Ger formatted BQ rows to match DB table schema"""
        rows = []
        for user, data in self.current_stats.items():
            row = {"user": user, "uuid": str(uuid.uuid4())}
            row.update(data)
            rows.append(row)
        return rows

    def get_unify_data(self):
        """Gets clients list for unifi controller and sets usage metrics per host/device"""
        with controller.API(username=UNIFI_USER, password=UNIFI_PWD) as api:
            device_list = api.list_clients()
        users_data = {}
        excluded = ['IPHONE', 'GALAXY', 'SAMSUNG', 'ONEPLUS', 'HUB', 'RASPBERRYPI', 'BRIDGE',
                    'PRINTER', 'DESKTOP', 'XIAOMI', 'POCOPHON', 'MI8', 'APPLE']
        for client in device_list:
            if 'hostname' in client and 'ap_mac' in client:
                hostname = client['hostname'].upper()
                is_excluded = any(device in hostname for device in excluded)
                if is_excluded:
                    continue
                clean_data = {
                    hostname.lower(): {
                        client['ap_mac']: ceil((client['last_seen'] - client['latest_assoc_time']) / 60)
                    }
                }
                users_data.update(clean_data)
        self.unifi_stats = users_data


def initial_setup():
    """Set up for local DB"""
    local_db = SQLite()
    local_db.create_table()


if __name__ == "__main__":
    try:
        # logging.info('Starting Job...')
        # initial_setup()
        # runner = SQLite()
        # result = runner.run_query('select * from local')
        # for row in result:
        #     print(row)
        server = OccupancyServer()
        server.get_unify_data()
        server.load_status_file()
        if datetime.today().hour == 19 and datetime.today().minute >= 45:
            server.join_user_data()
            server.update_site_users()
            bq_values = server.get_biq_query_rows()
            values = server.get_sql_rows()
            server.restart_daily_stats()
            bq_runner = BigQueryProcessor(SVC_ACCOUNT, DEST_PROJECT_ID, DEST_DATASET_ID,
                                          DEST_TABLE_ID, DEST_TABLE_SCHEMA, DEST_JSON_FILE)
            bq_runner.create_json_newline_delimited_file(bq_values)
            logging.info(f'Values: {bq_values}')
            bq_runner.load_data_from_file()
            db_runner = SQLite()
            db_runner.load_local_db(values)
            logging.info('Successfully finished Job')
    except Exception as e:
        logging.error(f'Error on cron job: {e}')
