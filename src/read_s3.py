import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('gdelt-open-data')
for obj in bucket.objects.all():
     key = obj.key
     body = obj.get()['Body'].read()
     print key
     print body
