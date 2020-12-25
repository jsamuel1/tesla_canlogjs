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
from dateutil.utils import default_tzinfo

logger = logging.getLogger()
logger.setLevel(logging.INFO)
debug = False
if (os.getenv('DEBUG')):
    logger.setLevel(logging.DEBUG)
    debug = True


write_client = aws.client('timestream-write')
s3_client = aws.client('s3')
s3 = aws.resource('s3')
sqs = aws.client('sqs')
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
        self.database_tables = []
        for val in result['Tables']:
            self.database_tables.append(val['TableName'])

        nextToken = result.get('NextToken')
        while nextToken:
            result = write_client.list_tables(DatabaseName=self.db_name, MaxResults=20, NextToken=nextToken)
            for val in result['Tables']:
                self.database_tables.append(val['TableName'])
            nextToken = result.get('NextToken')


    def ensure_table_exists(self, table):
        if table in self.database_tables:
            return

        logger.info(f"Table {table} doesn't exist.  Creating")

        try:
            write_client.create_table(DatabaseName=self.db_name, TableName=table, RetentionProperties={
                'MemoryStoreRetentionPeriodInHours': 8766,
                'MagneticStoreRetentionPeriodInDays': 73000
            })
        except write_client.exceptions.ConflictException as err:
            logger.info(f"Conflict Exception.  Table {table} already exists.")
            pass

        self.database_tables.append(table)  # add it to the cache


    def save_to_database(self, table, records, common_attributes = {}):
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
        logger.info("Started p[rocessing CSV.")
        msgrecords = {}
        for row in csvreader:
            try:
                if (debug):
                    logger.debug(f"{row[4]} MsgId: {row[1]} Data: {row[2]}")

                msg = dbc.get_message_by_frame_id(int(row[1],0))
                msgdata = msg.decode(a2b_hex(row[2][2:]))
                dt = default_tzinfo(datetime.strptime(row[4], r'%Y-%m-%dT%H:%M:%S.%f'), tzinfo)
                timeMilliseconds = str(int(dt.timestamp() * 1000))
                tableName = msg.name

                records = CanMsgToTimestreamSignal.extract_signals_to_records(dt, msg, msgdata, timeMilliseconds)

                stored_records = msgrecords.setdefault(tableName, [])
                if (len(stored_records) + len(records) > 99): # max batch size for timestream
                    logger.info(f'Saving to database as too many records for table {tableName}')
                    self.save_to_database(tableName, stored_records)
                    stored_records = []

                stored_records.extend(records)

            except KeyError as e:
                logger.warning(f"KeyError: {e} -- {row[4]}: MsgId: {row[1]} Data: {row[2]}")
                pass
            except ValueError as e:
                logger.error(f"ValueError: {e} -- {row[4]}: MsgId: {row[1]} Data: {row[2]}")
                pass
            except Exception as e:
                logger.exception(f"Exception: {e} -- MsgId: {row[1]} Data: {row[2]} DataLength: {row[3]}")
        
        logger.info(f"Finished processing CSV.")
        logger.info(f"Writing {len(msgrecords.items())} FrameIDs to database")
        for a_table, a_records in msgrecords.items():
            logger.info(f"Writing {a_table}: {len(a_records)}")
            self.save_to_database(a_table, a_records)
        logger.info("Finished writing FrameIDs to database")
        

    def extract_signals_to_records(dt, msg, msgdata, timeMilliseconds):
        dimensions = [{
            'Name': 'FrameId', 
            'Value': msg.name 
            }]
        records = []

        for sig, sigval in msgdata.items():
            if debug:
                logger.debug(f"{dt} : {msg.name} : {sig} : {str(sigval)}")

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
                                'Name': sig, 
                                'Value': str(sigval)
                            })
            else:
                records.append( {
                            'MeasureName': str(sig),
                            'MeasureValue': str(sigval),
                            'MeasureValueType': valueType,
                            'Time': timeMilliseconds
                        })

        if dimensions:
            for rec in records:
                rec.update({"Dimensions" : dimensions})
        return records

    def s3_download(self, bucket, key):
        obj = s3.Object(bucket, key)
        with TextIOWrapper(gzip.GzipFile(fileobj=obj.get()["Body"], mode='r')) as gzipfile:
            csvreader = csv.reader(gzipfile)
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


# for lambda
def handler(event, context):
     for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        processor = CanMsgToTimestreamSignal()
        # s3_select(bucket, key)
        processor.s3_download(bucket, key)

# for Docker / SQS
if __name__ == "__main__":
    queue_url = os.getenv("SQS_QUEUE_URL")
    response = sqs.receive_message(
        QueueUrl=queue_url, 
        MaxNumberOfMessages=1, 
        VisibilityTimeout=3600,
        WaitTimeSeconds=0)
    if not response.get('Messages'):
        exit(0)

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    logging.info(f'SQS Message received: {message}')
    if message.get('Body') and message['Body'].get('Detail'):
        requestParameters = message['Body']['Detail'].get('requestParameters')
        if ( not requestParameters):
            logging.warning(f"Request Body/Detail invalid.  {message['Body']['Detail']}")
            exit(0)

        bucket = requestParameters.get('bucketName')
        key = requestParameters.get('key')
        logging.info(f'processing {bucket} key {key}')

        processor = CanMsgToTimestreamSignal()
        processor.s3_download(bucket, key)

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logging.info('SQS Message processed: {message} ')

      
