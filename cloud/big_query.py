from google.cloud import bigquery
import os
import logging
import json


class BigQueryProcessor:
    def __init__(self, svc_account_path, project_id, dataset_id, dest_table, dest_table_schema, dest_json_file):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_account_path
        self.table_id = f'{project_id}.{dataset_id}.{dest_table}'
        self.svc_account = svc_account_path
        self.client = bigquery.Client()
        self.dest_table_schema = self.format_schema(dest_table_schema)
        self.dest_json_file = dest_json_file
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = svc_account_path

    @staticmethod
    def format_schema(schema):
        """
        Format table Schema
        :param schema: BigQuery table schema
        :return: Formatted Schema
        """
        formatted_schema = []
        for row in schema:
            formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))
        return formatted_schema

    def run_query(self, query):
        """
        Perform SQL to GCP BigQuery table
        :param query: SQL query
        :return: list of dicts records
        """
        try:
            logging.info(f"Running query: {query}")
            query_job = self.client.query(query)
            records = [dict(row) for row in query_job]
            return records
        except Exception as e:
            logging.error(f"Error processing query: {query}. Error was: {e}")

    def load_data_from_file(self):
        """
        Loads json newline delimited file to BigQuery table
        :return: None
        """
        try:
            logging.info("Loading data into BigQuery...")
            job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                                schema=self.dest_table_schema)
            job_config.ignore_unknown_values = True
            with open(self.dest_json_file, "rb") as source_file:
                job = self.client.load_table_from_file(source_file, self.table_id, job_config=job_config)
            print(job.result(), job.output_rows)
            logging.info("Loaded {} rows into {}.".format(job.output_rows, self.table_id))
        except Exception as e:
            logging.error(f"Error loading data to BigQuery. Error was: {e}")

    def create_json_newline_delimited_file(self, json_object):
        """
        Create a json delimited file from a json object, required format
        to load data from file to BigQuery table
        :param json_object: list/tuple iterable json object
        :return: None
        """
        # logging.info("Generating JSON file...")
        with open(self.dest_json_file, 'w') as f:
            for d in json_object:
                json.dump(d, f, default=str)
                f.write('\n')
        # logging.info(f"JSON file created at path: {self.dest_json_file}")