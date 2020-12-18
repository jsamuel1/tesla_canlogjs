import boto3 as aws
   
write_client = aws.client('timestream-write')

class TimeStreamDeleteTables(object):

    db_name = ''
    database_tables = []

    def __init__(self, dbName):
        self.db_name = dbName
        self.populate_database_tables()
        self.delete_all_tables()


    def populate_database_tables(self):
        result = write_client.list_tables(DatabaseName=self.db_name, MaxResults=20)
        self.database_tables = result['Tables']
        nextToken = result.get('NextToken')
        while nextToken:
            result = write_client.list_tables(DatabaseName=self.db_name, MaxResults=20, NextToken=nextToken)
            self.database_tables.extend(result['Tables'])
            nextToken = result.get('NextToken')


    def delete_all_tables(self):
        for table_props in self.database_tables:
            write_client.delete_table(DatabaseName=self.db_name, TableName=table_props["TableName"])

if __name__ == "__main__":
    TimeStreamDeleteTables('teslacanbus')
    

