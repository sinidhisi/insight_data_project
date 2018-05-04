from confluent_kafka import Producer
import time
import boto3

p = Producer({'bootstrap.servers':'ec2-54-70-242-121.us-west-2.compute.amazonaws.com:9092'  })

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def read_S3(bucket_name):
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(bucket_name)
	p.poll(0)
	for obj in bucket.objects.all():
    	  key = obj.key
   	  body = obj.get()['Body']
          data = body.read().splitlines()		
	  for item in data:
	  	p.produce('gdelt_topic',item, callback=delivery_report)
	        time.sleep(1)
#   	  print body
	
        p.flush()

read_S3('gdelt-open-data/events/*')
some_data_source = ['doing this not s3']
for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)
# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()
