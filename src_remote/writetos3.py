import boto3

some_binary_data = b'Here we have some data'
more_binary_data = b'Here we have some more data'

# Method 1: Object.put()
s3 = boto3.resource('s3')
object = s3.Object('xetra-data', 'filename.txt')
object.put(Body=some_binary_data)
