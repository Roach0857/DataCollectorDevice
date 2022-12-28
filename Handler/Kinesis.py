import json
import logging
import time

import boto3
from botocore.exceptions import ClientError

class Handle():
    def __init__(self, logger):
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        self.boto3Client = boto3.client('kinesis', region_name=self.session.region_name,
                                      aws_access_key_id=self.credentials.access_key,
                                      aws_secret_access_key=self.credentials.secret_key)
        self.kinesisStream = KinesisStream(self.boto3Client, logger)
    
class KinesisStream:
    """Encapsulates a Kinesis stream."""
    def __init__(self, kinesis_client, logger):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.logger = logger
        self.kinesis_client = kinesis_client
        self.name = None
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def _clear(self):
        """
        Clears property data of the stream object.
        """
        self.name = None
        self.details = None

    def arn(self):
        """
        Gets the Amazon Resource Name (ARN) of the stream.
        """
        return self.details['StreamARN']

    def create(self, name, wait_until_exists=True):
        """
        Creates a stream.

        :param name: The name of the stream.
        :param wait_until_exists: When True, waits until the service reports that
                                  the stream exists, then queries for its metadata.
        """
        try:
            self.kinesis_client.create_stream(StreamName=name, ShardCount=1)
            self.name = name
            self.logger.info(f"Created stream {name}.", name)
            if wait_until_exists:
                self.logger.info("Waiting until exists.")
                self.stream_exists_waiter.wait(StreamName=name)
                self.describe(name)
        except ClientError:
            self.logger.error(f"Couldn't create stream {name}.")
            raise

    def describe(self, name):
        """
        Gets metadata about a stream.

        :param name: The name of the stream.
        :return: Metadata about the stream.
        """
        for i in range(3):
            try:
                response = self.kinesis_client.describe_stream(StreamName=name)
                self.name = name
                self.details = response['StreamDescription']
                self.logger.info(f"Got stream {name}.")
                return self.details
            except ClientError:
                self.logger.error(f"Couldn't get {name}.")
            except Exception as ex:
                self.logger.error(f"Describe stream faild {ex}.") 
            time.sleep(1)

    def delete(self):
        """
        Deletes a stream.
        """
        try:
            self.kinesis_client.delete_stream(StreamName=self.name)
            self._clear()
            self.logger.info(f"Deleted stream {self.name}.")
        except ClientError:
            self.logger.error(f"Couldn't delete stream {self.name}.")
            raise

    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.name,
                Data=json.dumps(data),
                PartitionKey=partition_key)
            self.logger.info(f"Put record in stream {self.name}.")
        except ClientError:
            self.logger.error(f"Couldn't put record in stream {self.name}.")
            raise
        else:
            return response
    
    def put_records(self, data):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_records(
                StreamName=self.name,
                Records=[
                    {
                        'Data': json.dumps(d),
                        'PartitionKey': self.details['Shards'][0]['ShardId']
                    }
                    for d in data] 
                )
            self.logger.info(f"Put record in stream {self.name}.", )
        except ClientError:
            self.logger.error(f"Couldn't put record in stream {self.name}." )
            raise
        else:
            return response

    def get_records(self, max_records):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.name, ShardId=self.details['Shards'][0]['ShardId'],
                ShardIteratorType='LATEST')
            shard_iter = response['ShardIterator']
            record_count = 0
            while record_count < max_records:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter, Limit=10)
                shard_iter = response['NextShardIterator']
                records = response['Records']
                self.logger.info(f"Got {len(records)} records.")
                record_count += len(records)
                yield records
        except ClientError:
            self.logger.error(f"Couldn't get records from stream {self.name}.", self.name)
            raise
      
    def get_shards(self):
        try:
            response = self.kinesis_client.list_shards(StreamName=self.name)
            self.logger.info(f"Get shards in stream {self.name}.", )
            self.details["Shards"] = response["Shards"]
        except ClientError:
            self.logger.error(f"Couldn't get shards in stream {self.name}.")
            raise
        else:
            return response