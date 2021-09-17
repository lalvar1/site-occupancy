import sqlite3
import logging
import os

module_path = os.path.dirname(os.path.realpath(__file__))

class SQLite:
    """Simple class to backup data in local SQLite"""
    def __init__(self):
        self.db_name = f'{module_path}/local.db'

    def load_local_db(self, row_values):
        """Loads multiple rows """
        try:
            con = sqlite3.connect(self.db_name)
            cur = con.cursor()
            logging.info('Loading local db')
            query = "INSERT INTO local (user, spent, date, uuid) VALUES (?,?,?,?)"
            cur.executemany(query, row_values)
            # logging.warning(f'Committing values: {values}')
            con.commit()
            con.close()
        except Exception as e:
            logging.error(f'Error while running query: {e}')

    def run_query(self, query, commit=False):
        """Run SQL query"""
        try:
            con = sqlite3.connect(self.db_name)
            cur = con.cursor()
            logging.info(f'Running query: {query}')
            result = cur.execute(query)
            if commit:
                logging.warning(f'Committing values: {query}')
                con.commit()
            # con.close()
            return result
        except Exception as e:
            logging.error(f'Error while running query: {e}')

    def create_table(self):
        """Initial DB creation"""
        try:
            con = sqlite3.connect(self.db_name)
            cur = con.cursor()
            logging.info(f'Creating Table {self.db_name}')
            cur.execute('''CREATE TABLE local
                           (user text, spent real, date text, uuid text)''')
            # cur.execute(f'''CREATE TABLE {name}
            #                {fields}''')
            logging.info(f'Table {self.db_name} Created')
            con.close()
        except Exception as e:
            logging.error(f'Failed creating table {self.db_name}. Error: {e}')
