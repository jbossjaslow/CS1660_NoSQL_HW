import boto3
import botocore
import sys
import csv
import json

bucket_name = 'datacont-josh'
server_name = 'us-west-2'

boto3.setup_default_session(region_name = server_name)

s3 = boto3.resource('s3',
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name = server_name)

try:
    bucket = s3.create_bucket(Bucket = bucket_name,
        CreateBucketConfiguration = {
            'LocationConstraint': server_name
        }
    )
    bucket.Acl().put(ACL = 'public-read')

except botocore.exceptions.ClientError as err:
    # print("This bucket already exists")
    if err.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        bucket = s3.Bucket(bucket_name)

except Exception as e:
    sys.exit(e)

# Upload a file, 'test.jpg' into the newly created bucket
# s3.Object(bucket_name, 'test.jpg').put(Body = open('./test.jpg','rb'))

dyndb = boto3.resource('dynamodb',
    aws_access_key_id='AKIA56R6KGFX5MWQRPW5',
    aws_secret_access_key='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',
    region_name = server_name)

table_name = 'DataTable-josh'

# The first time that we define a table, we use:
try:
    table = dyndb.create_table(
        TableName = table_name,
        KeySchema = [
            { 'AttributeName': 'PartitionKey', 'KeyType': 'HASH' },
            { 'AttributeName': 'RowKey', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions = [
            { 'AttributeName': 'PartitionKey', 'AttributeType': 'S' },
            { 'AttributeName': 'RowKey', 'AttributeType': 'S' }
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

except botocore.exceptions.ClientError as err:
    if err.response['Error']['Code'] == 'ResourceInUseException':
        # If the table has been previously defined, use:
        table = dyndb.Table(table_name)
    else:
        sys.exit(err.response)

except Exception as e:
    sys.exit(e)

# Wait for the table to be created
try:
    table.meta.client.get_waiter('table_exists').wait(TableName = table_name)

except botocore.exceptions.NoCredentialsError as e:
    sys.exit(e)

except Exception as e:
    sys.exit(e)

# CSV manipulation
urlbase = "https://s3-" + server_name + ".amazonaws.com/" + bucket_name + "/"
with open('./experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter = ',', quotechar = '|')
    for item in csvf:
        body = open('./datafiles/'+item[3], 'rb')
        s3.Object(bucket_name, item[3]).put(Body = body)
        md = s3.Object(bucket_name, item[3]).Acl().put(ACL = 'public-read')

        # Set URL for the data file to be publicly readable
        url = urlbase + item[3]

        metadata_item = {
            'PartitionKey': item[0],
            'RowKey': item[1],
            'description': item[4],
            'date': item[2],
            'url': url
        }

        try:
            table.put_item(Item = metadata_item)
        except Exception as e:
            sys.exit(e)

# Search for an Item
choice = '3'
try:
    response = table.get_item(
        Key = {
            'PartitionKey': 'experiment' + choice,
            'RowKey': 'data' + choice
        }
    )
except Exception as e:
    sys.exit("Error" + e)

item = response['Item']
print(json.dumps(item, indent=4))
