import requests
import urllib3
import logging

UNIFI_LOGIN_PATH = '/api/auth/login'
urllib3.disable_warnings()
logging.getLogger("requests").setLevel(logging.WARNING)


class API(object):
    def __init__(self, username: str = "user", password: str = "Str0ngP4sS!",
                 site: str = "default", host: str = "unifi"):
        self.host = host
        self.site = site
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.csrf = ""

    def __enter__(self):
        """
        Contextmanager entry handle
        :return: instance object of class
        """
        self.login()
        return self

    def login(self):
        payload = {
            "username": self.username,
            "password": self.password,
        }
        r = self.request(UNIFI_LOGIN_PATH, payload)
        if r.status_code != 200:
            raise logging.error("Failed to log in to API with provided credentials")
        print(r.status_code)
        return r.ok

    def request(self, path, data={}, method='POST'):
        """Make http request to specified endpoint"""
        uri = 'https://{}{}'.format(self.host, path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }
        if self.csrf:
            headers["X-CSRF-Token"] = self.csrf
        try:
            r = getattr(self.session, method.lower())(uri, json=data, verify=False, headers=headers)
            self.csrf = r.headers['X-CSRF-Token']
            return r
        except KeyError:
            pass

    def list_clients(self):
        """Get list of all active clients"""
        try:
            b = self.request(f'/proxy/network/api/s/{self.site}/stat/sta')
            data = b.json()['data']
            return data
        except Exception as e:
            logging.error(f'Error while getting data from controller: {e}')
