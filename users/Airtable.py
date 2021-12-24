import requests
import logging

logging.getLogger("airtable").setLevel(logging.WARNING)


class Airtable:
    def __init__(self, api_key, table_name, base_id):
        """
        Airtable Class
        Args:
            base_id (``str``): Airtable API Key.
            table_name (``str``): Airtable API Key.
            api_key (``str``): Airtable API Key.
        """
        self.api_key = api_key
        self.auth_token = {"Authorization": f"Bearer {self.api_key}"}
        self.table_name = table_name
        self.base_id = base_id
        self.url = f"https://api.airtable.com/v0/{self.base_id}/{self.table_name}"

    def get_users(self):
        """Get all users, full name + guid"""
        # logging.info("Getting users from Airtable...")
        try:
            user_response = requests.get(self.url, headers=self.auth_token).json()
            user_data = {
                    user['fields']['Username'].lower(): {
                        'full_name': user['fields']['Full Name'],
                        'location': user['fields']['Geography'].lower(),
                        'remote': True if 'Managed accounts' in user['fields']
                                          and user['fields']['Managed accounts'].upper() == 'NO' else False
                    }
                    for user in user_response['records']}
            return user_data
        except Exception as e:
            logging.exception(f"Error while getting users from Airtable: {e}")
