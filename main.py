import logging
from datetime import datetime
import os
import json
import uuid
import urllib3
from math import ceil
from dotenv import load_dotenv
from cloud.big_query import BigQueryProcessor
from local_db import SQLite
import UDM.unify as controller
# import UDM_PRO.unify as controller


load_dotenv()
urllib3.disable_warnings()
UNIFI_USER = os.environ['USERNAME']
UNIFI_PWD = os.environ['PASSWORD']
SVC_ACCOUNT = './svc_account_occupancy.json'
DEST_PROJECT_ID = 'fenix-occupancy-data-server'
DEST_DATASET_ID = 'site_occupancy'
DEST_TABLE_ID = 'bcn'
DEST_TABLE_SCHEMA = {'name': 'user', 'type': 'STRING', 'mode': 'REQUIRED'}, \
                    {'name': 'spent', 'type': 'INTEGER', 'mode': 'NULLABLE'}, \
                    {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'}, \
                    {'name': 'uuid', 'type': 'STRING', 'mode': 'REQUIRED'}
DEST_JSON_FILE = './bq_user_updates.json'

logging.basicConfig(filename='automation.log', level=logging.DEBUG,
                    format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class OccupancyServer:
    """
    Periodically queries users occupancy from
    Unify Controller and updates daily user metrics
    """
    def __init__(self):
        self.unify_host = 'https://unifi:8443'
        self.daily_stats_file = './daily_stats.json'
        self.current_stats = self.get_users_file_data()
        self.unifi_stats = {}

    def get_users_file_data(self):
        """Read current daily stats from file"""
        f = open(self.daily_stats_file)
        data = json.load(f)
        return data

    def load_status_file(self):
        """Updates local file based on current and unifi stats"""
        logging.info("Updating JSON file...")
        for user, data in self.unifi_stats.items():
            self.current_stats[user] = data
        with open(self.daily_stats_file, 'w') as f:
            f.write(json.dumps(self.current_stats, indent=4))
        f.close()
        logging.info("JSON file updated")

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
            device_list = (api.list_clients(order_by="ip"))
        users_data = {}
        for client in device_list:
            if 'hostname' in client:
                hostname = client['hostname'].upper()
                excluded = ['IPHONE', 'GALAXY', 'SAMSUNG', 'ONEPLUS', 'HUB', 'RASBERRYPI', 'NUKI_BRIDGE_201F6482',
                            'FENIX-BCN-PRINTER', 'DESKTOP', 'XIAOMI']
                is_excluded = any(device in hostname for device in excluded)
                if is_excluded:
                    continue
                clean_data = {
                    client['hostname']:
                        {
                        'spent': ceil((client['last_seen'] - client['assoc_time']) / 60),
                        'date': datetime.today().strftime('%Y-%m-%d')
                    }
                }
                users_data.update(clean_data)
        self.unifi_stats = users_data


def initial_setup():
    """Set up for local DB"""
    local_db = SQLite()
    local_db.create_table()


if __name__ == "__main__":
    logging.info('Starting Job...')
    server = OccupancyServer()
    server.get_unify_data()
    server.load_status_file()
    # result = runner.run_query('select * from local')
    # for row in result:
    #     print(row)
    if datetime.today().hour >= 19:
        values = server.get_sql_rows()
        db_runner = SQLite()
        db_runner.load_local_db(values)
        bq_values = server.get_biq_query_rows()
        bq_runner = BigQueryProcessor(SVC_ACCOUNT, DEST_PROJECT_ID, DEST_DATASET_ID,
                                      DEST_TABLE_ID, DEST_TABLE_SCHEMA, DEST_JSON_FILE)
        bq_runner.create_json_newline_delimited_file(bq_values)
        bq_runner.load_data_from_file()
    logging.info('Successfully finished Job!')
