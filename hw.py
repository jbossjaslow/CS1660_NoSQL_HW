import boto3
import botocore
import sys
import csv

s3 = boto3.resource('s3',
    aws_access_key_id='AKIA56R6KGFX5MWQRPW5',
    aws_secret_access_key='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')

bucketName = 'datacont-josh'
serverName = 'us-east-2'

try:
    s3.create_bucket(Bucket = bucketName,
        CreateBucketConfiguration = {
            'LocationConstraint': serverName
        }
    )
except:
    print("This bucket already exists")

bucket = s3.Bucket(bucketName)
bucket.Acl().put(ACL = 'public-read')

# Upload a file, 'test.jpg' into the newly created bucket
s3.Object('datacont', 'test.jpg').put(Body = open('./test.jpg','rb'))

dyndb = boto3.resource('dynamodb', region_name = serverName)

# The first time that we defina a table, we use:
try:
    table = dyndb.create_table(
        TableName = 'DataTable',
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
except:
    # If the table has been previously defined, use:
    table = dyndb.Table("DataTable")

# Wait for the table to be created
try:
    table.meta.client.get_waiter('table_exists').wait(TableName = 'DataTable')

except botocore.exceptions.NoCredentialsError:
    sys.exit("Error with credentials")

except:
    print("Other error")

# CSV manipulation
urlbase = "https://s3-" + serverName + ".amazonaws.com/" + bucketName + "/"
with open('./experiments.csv', 'rb') as csvfile:
    csvf = csv.reader(csvfile, delimited = ',', quotechar = '|')
    for item in csvf:
        body = open('./datafiles\\'+item[3], 'rb')
        s3.Object(bucketName, item[3].put(Body = body))
        md = s3.Object(bucketName, item[3]).Acl().put(ACL = 'public-read')

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
        except:
            print("Item may already be there or another failure")

# Search for an Item
response = table.get_item(
    Key = {
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)
