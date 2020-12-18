import unittest
from binascii import a2b_hex
from datetime import datetime
from pprint import pprint
from unittest import TestCase

import cantools
from dateutil import tz
from dateutil.utils import default_tzinfo
from processing import canmsgtosignals


class CanMsgToSignalsTests(TestCase):


    @classmethod
    def setup_class(self):
        self.dbc = cantools.database.load_file('../processing/Model3CAN.dbc')

        self.testmessages = [ 
            ['ID2E1VCFRONT_status', '0x2e1', '0x010c03ff1f050000'],
            ['D2B3VCRIGHT_logging1Hz', '0x2b3', '0x04adad8a8a000000']  # Multiplexed
        ]

    @classmethod
    def teardown_class(self):
        pass

    def test_extract_signals_to_records(self):
        tzinfo = tz.gettz('Australia/Melbourne')
        dt = default_tzinfo(datetime.now(), tzinfo)
        timeMilliseconds = str(int(dt.timestamp() * 1000))

        for message, frameid, data in self.testmessages:        
            msg = self.dbc.get_message_by_frame_id(int(frameid,0))
            msgdata = msg.decode(a2b_hex(data[2:]))
            records = canmsgtosignals.CanMsgToTimestreamSignal.extract_signals_to_records(dt, msg, msgdata, timeMilliseconds)
            assert len(records) > 0
            pprint(records)
        
        

if __name__ == "__main__":
    unittest.main()






