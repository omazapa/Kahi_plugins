from kahi.KahiBase import KahiBase
from pymongo import MongoClient, TEXT
from pandas import read_excel
from time import time
from kahi_impactu_utils.String import title_case


class Kahi_dspace_works(KahiBase):

    config = {}

    def __init__(self, config):
        self.config = config

        self.client = MongoClient(config["database_url"])

        self.db = self.client[config["database_name"]]

        self.verbose = config["verbose"] if "verbose" in config else 0

    def run(self):

        return 0
