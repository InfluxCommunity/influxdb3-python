import os
import json


class Config:
    def __init__(self):
        self.host = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
        self.token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
        self.database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    def __str__(self):
        return json.dumps(self.__dict__)
