import csv
import gzip
import logging
import os
from binascii import a2b_hex
from datetime import datetime
from io import StringIO, TextIOWrapper
from urllib.parse import unquote_plus

import boto3 as aws
import cantools
import dateutil
from aws_xray_sdk.core import patch_all, xray_recorder
from dateutil.utils import default_tzinfo

patch_all()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if (os.getenv('DEBUG')):
    logger.setLevel(logging.DEBUG)

write_client = aws.client('timestream-write')
s3_client = aws.client('s3')
s3 = aws.resource('s3')
dbc = cantools.database.load_file('Model3CAN.dbc')
tzinfo = dateutil.tz.gettz(os.getenv('TZ'))

class CanMsgToTimestreamSignal(object):

    db_name = ''
    database_tables = []

    def __init__(self):
        self.db_name = os.getenv('TSDB_NAME')
        self.populate_database_tables()

    def populate_database_tables(self):
        result = write_client.list_tables(DatabaseName=self.db_name, MaxResults=20)
        self.database_tables = result['Tables']
        nextToken = result.get('NextToken')
        while nextToken:
            result = write_client.list_tables(DatabaseName=self.db_name, MaxResults=20, NextToken=nextToken)
            self.database_tables.extend(result['Tables'])
            nextToken = result.get('NextToken')


    def ensure_table_exists(self, table):
        exists = [table_props for table_props in self.database_tables if table_props["TableName"] == table]
        if (exists):
            return

        try:
            write_client.create_table(DatabaseName=self.db_name, TableName=table, RetentionProperties={
                'MemoryStoreRetentionPeriodInHours': 8766,
                'MagneticStoreRetentionPeriodInDays': 73000
            })
        except write_client.exceptions.ConflictException as err:
            logger.info(f"Conflict Exception.  Table {table} already exists.")
            pass

        self.populate_database_tables()


    def save_to_database(self, table, records, common_attributes):
        if not records:
            return

        self.ensure_table_exists(table)
        try:
            result = write_client.write_records(DatabaseName=self.db_name, TableName=table, Records=records, CommonAttributes=common_attributes)
        except write_client.exceptions.RejectedRecordsException as err:
            logger.error("RejectedRecords: " + str(err))
            logger.error("RejectedRecords Response: " + str(err.response))
            logger.error("RejectedRecords Error: " + str(err.response["Error"]))
            for rr in err.response["RejectedRecords"]:
                logger.error("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
        except Exception as err:
            logger.exception(f"Error: {err}")

    def process_messages(self, dbc, csvreader):
        csvreader.__next__()
        for row in csvreader:
            try:
                logger.debug(f"{row[4]} MsgId: {row[1]} Data: {row[2]}")
                # msg = dbc.decode_message(int(row[1],0), a2b_hex(row[2][2:]))
                msg = dbc.get_message_by_frame_id(int(row[1],0))
                msgdata = msg.decode(a2b_hex(row[2][2:]))

                dimensions = []

                tableName = msg.name

                dt = default_tzinfo(datetime.strptime(row[4], r'%Y-%m-%dT%H:%M:%S.%f'), tzinfo)
                timeMilliseconds = str(int(dt.timestamp() * 1000))

                records = []
                self.extract_signals_to_records(row, msg, msgdata, dimensions, records)


                common_attributes = {
                    'Dimensions': dimensions,
                    'Time': timeMilliseconds
                }

                self.save_to_database(tableName, records, common_attributes)

            except KeyError as e:
                logger.warning(f"KeyError: {e} -- {row[4]}: MsgId: {row[1]} Data: {row[2]}")
                pass
            except ValueError as e:
                logger.error(f"ValueError: {e} -- {row[4]}: MsgId: {row[1]} Data: {row[2]}")
                pass
            except Exception as e:
                logger.exception(f"Exception: {e} -- MsgId: {row[1]} Data: {row[2]} DataLength: {row[3]}")

    def extract_signals_to_records(self, row, msg, msgdata, dimensions, records):
        for sig, sigval in msgdata.items():
            logger.debug(f"{row[4]} {row[1]} : {sig} : {str(sigval)}")
            multiplex = False
            if msg.is_multiplexed():
                signal = msg.get_signal_by_name(sig)
                if signal.is_multiplexer:
                    multiplex = True
            valueType = 'VARCHAR'
            if type(sigval) == float:
                valueType = 'DOUBLE'
            elif type(sigval) == int:
                valueType = 'BIGINT'
            elif type(sigval) == bool:
                valueType = 'BOOLEAN'
            if multiplex == True:
                dimensions.append(
                            {
                                'Name': sig, 'Value': str(sigval)
                            })
            else:
                records.append( {
                            'MeasureName': str(sig),
                            'MeasureValue': str(sigval),
                            'MeasureValueType': valueType
                        })

    def s3_download(self, bucket, key):
        subsegment = xray_recorder.begin_subsegment('DownloadFile')
        subsegment.put_annotation('key', key)
        obj = s3.Object(bucket, key)
        with TextIOWrapper(gzip.GzipFile(fileobj=obj.get()["Body"], mode='r')) as gzipfile:
            csvreader = csv.reader(gzipfile)
            xray_recorder.end_subsegment()
            self.process_messages(dbc, csvreader)


    def s3_select(self, bucket, key):
        req = s3_client.select_object_content(
                Bucket=bucket, Key=key,
                ExpressionType='SQL',
                Expression='select * from s3object',
                InputSerialization = {'CompressionType': 'GZIP', 'CSV': { 'FieldDelimiter': ',', 'RecordDelimiter': '\n', 'FileHeaderInfo': 'USE'}},
                OutputSerialization = {'CSV': { 'FieldDelimiter': ',', 'RecordDelimiter': '\n'}}
            )
        for event in req['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                file_str = StringIO(''.join(r for r in records))
                csvreader = csv.reader(file_str)
                self.process_messages(dbc, csvreader)
            elif 'Stats' in event:
                logging.info(f"Bytes scanned: {event['Stats']['Details']} Processed: {event['Stats']['Details']}")


def handler(event, context):
     for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        processor = CanMsgToTimestreamSignal()
        # s3_select(bucket, key)
        processor.s3_download(bucket, key)

