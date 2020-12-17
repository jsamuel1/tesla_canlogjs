#!/usr/bin/env python3

import csv
import sys
from datetime import datetime, timedelta

from panda import Panda

def can_logger():

  try:
    print("Trying to connect to Panda over USB...")
    p = Panda()

  except AssertionError:
    print("USB connection failed. Trying WiFi...")

    try:
      p = Panda("WIFI")
    except Exception:
      print("WiFi connection timed out. Please make sure your Panda is connected and try again.")
      sys.exit(0)

  bus0_msg_cnt = 0
  bus1_msg_cnt = 0
  bus2_msg_cnt = 0
  print("Writing CAN bus data locally,look for a csv file in the current directory, Press Ctrl-C to exit...\n")

  try:
    while True:
      start_datetime = datetime.now()

      with open('canbus-'+ start_datetime.strftime("%Y%m%d-%H%M%S") + '.csv', 'w') as outputfile:
        csvwriter = csv.writer(outputfile)

        # Write Header
        csvwriter.writerow(['Bus', 'MessageID', 'Message', 'MessageLength', 'DateTime'])

        while datetime.now() < start_datetime + timedelta(minutes=60):
          can_recv = p.can_recv()

          for address, _, dat, src in can_recv:
            csvwriter.writerow([str(src), str(hex(address)), f"0x{dat.hex()}", len(dat), datetime.now().isoformat()])

            if src == 0:
              bus0_msg_cnt += 1
            elif src == 1:
              bus1_msg_cnt += 1
            elif src == 2:
              bus2_msg_cnt += 1

        print(f"Message Counts... Bus 0: {bus0_msg_cnt} Bus 1: {bus1_msg_cnt} Bus 2: {bus2_msg_cnt}", end='\r')

  except KeyboardInterrupt:
    print(f"\nNow exiting. Final message Counts... Bus 0: {bus0_msg_cnt} Bus 1: {bus1_msg_cnt} Bus 2: {bus2_msg_cnt}")

if __name__ == "__main__":
  can_logger()

