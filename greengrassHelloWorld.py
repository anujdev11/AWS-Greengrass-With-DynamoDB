import greengrasssdk
import platform
from threading import Timer
import boto3
import logging
from datetime import datetime
from random import *
from botocore.exceptions import ClientError


# Creating a greengrass core sdk client
client = greengrasssdk.client('iot-data')

# Retrieving platform information to send from Greengrass Core
my_platform = platform.platform()


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
tableName = "receiveData1"

dynamodb_client = boto3.client('dynamodb')
existing_tables = dynamodb_client.list_tables()['TableNames']

try:
    table = dynamodb.create_table(
        TableName=tableName,
        KeySchema=[
            {
                'AttributeName': 'Raspberry_Id', 
                'KeyType': 'HASH'  #Partition key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Raspberry_Id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
except ClientError as e:
    if e.response['Error']['Code'] == 'ResourceInUseException':
        print("Table already created")
    else:
        raise e

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def greengrass_hello_world_run():
    f=open("/home/pi/rf24libs/RF24/examples_linux/datareceive.txt","r")
    receiveddata=f.read()
    f.close()
    if not my_platform:
        client.publish(
            topic='hello/world2',
            payload='Hello world! Sent from Greengrass Core.')
    else:
        client.publish(
            topic='hello/world2',
            payload='Data is :{}'.format(receiveddata))
        table = dynamodb.Table(tableName)
        if tableName not in existing_tables:
            table.put_item(
            Item={
                'Raspberry_Id':'0000000068480410',
                'Time':str(datetime.utcnow()),
                'Received_Data':str(receiveddata),
                }
            )
        else:
            table.update_item(
                    Key={
                        'Raspberry_Id':'0000000068480410',
                    },
                UpdateExpression='SET Received_Data = :val1',
                    ExpressionAttributeValues={
                        ':val1': str(receiveddata)
                    }
                )
                    

    # Asynchronously schedule this function to be run again in 5 seconds
    Timer(5, greengrass_hello_world_run).start()


# Start executing the function above

greengrass_hello_world_run()

# This is a dummy handler and will not be invoked
# Instead the code above will be executed in an infinite loop for our example
def function_handler(event, context):
    return