from binascii import a2b_hex
import cantools
import csv
import os
from pprint import pprint
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile


if __name__ == "__main__":
    dbc = cantools.database.load_file('Model3CAN.dbc')
    dbc.add_dbc_file('tesla_model3.dbc')
    dbc.add_dbc_file('Model3CAN.dbc')
    cantools.database.dump_file(dbc, 'Model3CAN.dbc')

