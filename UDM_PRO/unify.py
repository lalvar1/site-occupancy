import requests
import urllib3
import logging

UNIFI_LOGIN_PATH = '/api/auth/login'
urllib3.disable_warnings()


class API(object):
    def __init__(self, username: str = "user", password: str = "Str0ngP4sS!",
                 site: str = "default", host: str = "unifi"):
        self.host = host
        self.site = site
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.csrf = ""
        logging.getLogger("requests").setLevel(logging.INFO)

    def __enter__(self):
        """
        Contextmanager entry handle
        :return: instance object of class
        """
        self.login()
        return self

    def __exit__(self, *args, **kwargs):
        """
        Log user out
        :return: none
        """
        self.logout()

    def logout(self):
        """
        Log the user out
        :return: None
        """
        self.request("/logout", method='GET')
        self.session.close()

    def login(self):
        payload = {
            "username": self.username,
            "password": self.password,
        }
        r = self.request(UNIFI_LOGIN_PATH, payload)
        if r.status_code != 200:
            logging.error("Failed to log in to API with provided credentials")
            raise Exception('Auth Error')
        return r.ok

    def request(self, path, data={}, method='POST'):
        uri = 'https://{}{}'.format(self.host, path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }

        if self.csrf:
            headers["X-CSRF-Token"] = self.csrf

        r = getattr(self.session, method.lower())(uri, json=data, verify=False, headers=headers)
        try:
            self.csrf = r.headers['X-CSRF-Token']
        except KeyError:
            pass
        return r

    def list_clients(self):
        """Get list of all active clients"""
        try:
            b = self.request(f'/proxy/network/api/s/{self.site}/stat/sta')
            data = b.json()['data']
            return data
        except Exception as e:
            logging.error(f'Error while getting data from controller: {e}')
