from confluent_kafka import Consumer, KafkaError


c = Consumer({
    'bootstrap.servers':'ec2-54-218-205-93.us-west-2.compute.amazonaws.com:9092' ,
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

c.subscribe(['gdelt_topic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    print('Received message: {}'.format(msg.value()))

c.close()
