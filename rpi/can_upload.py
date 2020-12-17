#!/usr/bin/env python3

import os
from os import path
import subprocess as sub
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError
from time import sleep

def upload_to_aws(local_file, s3_file):
    access_key = "AXIAXXXXXXXXXXXXXXXX"
    secret_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
    s3_bucket = "bucket1"
    return upload_to_aws_proc(access_key, secret_key, local_file, s3_bucket, s3_file)

def upload_to_aws_proc(access_key, secret_key, local_file, s3_bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
    try:
        s3.upload_file(local_file, s3_bucket, s3_file)
        print("Upload Successful to this location :",s3_bucket+"/"+s3_file )
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def file_older_than(file_name):
    filetime = datetime.fromtimestamp(path.getctime(file_name))
    olderthan = datetime.now() - timedelta(minutes=61)
    bOlderThan = filetime < olderthan
    return bOlderThan

def internet_connected():
    return sub.run(["ping", "-c", "1", "8.8.8.8"], stdout=sub.DEVNULL, stderr=sub.DEVNULL).returncode == 0

def compress_file(file_name):
    sub.run(["gzip", "-9", file_name], stdout=sub.DEVNULL, stderr=sub.DEVNULL)
    return file_name + ".gz"

if __name__ == "__main__":
  while True:
    if internet_connected():
        for file_name in os.listdir():
            if file_name.endswith(".csv") and file_older_than(file_name):
                print(f"Uploading {file_name}")
                file_name = compress_file(file_name)
            if file_name.endswith(".csv.gz") and internet_connected():
                bsucess = upload_to_aws(file_name, 'canlog' + '/' + file_name ) and bsucess
                if bsucess:
                    print(f"removing archived file: {file_name}")
                    os.remove(file_name)
    else:
        sleep(60)

