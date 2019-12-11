from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import OFFSET_BEGINNING
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.create(index='prgs1min', ignore=400)


c = AvroConsumer({'default.topic.config': {'auto.offset.reset': 'earliest'},'bootstrap.servers': 'ip adress:9092' ,'group.id': 'test-consumer-group', 'schema.registry.url': 'http://localhost:8081'})

def my_assign (consumer, partitions):
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    print('assign', partitions)
    consumer.assign(partitions)

c.subscribe(['PRGS1MIN'],  on_assign=my_assign)
running = True

while running:
    try:
        print("\nPolling Data----------------------------------")
        msg = c.poll(10)
        print(msg)
        if msg is None:
            print("Message is None")
            continue
        else:
            if not msg.error():
                print(msg.value())
                es.index(index="prgs1min", id=msg.value().get('KEY'), body=msg.value())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False

c.close()
