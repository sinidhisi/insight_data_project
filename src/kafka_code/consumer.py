from confluent_kafka import Consumer, KafkaError


c = Consumer({
    'bootstrap.servers': 'ec2-54-214-142-234.us-west-2.compute.amazonaws.com',
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

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
