import os
import json


class Config:
    def __init__(self):
        self.host = os.getenv('INFLUXDB_HOST') or 'https://us-east-1-1.aws.cloud2.influxdata.com/'
        self.token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
        self.org = os.getenv('INFLUXDB_ORG') or 'my-org'
        self.database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    def __str__(self):
        return json.dumps(self.__dict__)
