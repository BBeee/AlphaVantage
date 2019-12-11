from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import OFFSET_BEGINNING
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

sc = SparkContext()
sc. setLogLevel("WARN")
sqlContext = HiveContext(sc)
sqlContext.sql("CREATE TABLE IF NOT EXISTS test(symbol STRING, key STRING, open DOUBLE, close DOUBLE)")

c = AvroConsumer({'default.topic.config': {'auto.offset.reset': 'earliest'},'bootstrap.servers': 'ip adress:9092' ,'group.id': 'test-consumer-group', 'schema.registry.url': 'http://ip adress:8081'})

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
        if msg is None:
            print("Message is None")
            continue
        else:
            if not msg.error():
                key = (msg.value().get('KEY'))
                close = (msg.value().get('CLOSE_'))
                open = (msg.value().get('OPEN_'))
                symbol = str(msg.value().get('QUERY_SYMBOL'))
                print(" '%s','%d','%.4f','%.4f'"%(symbol,key,open,close))
                sqlContext.sql("INSERT INTO TABLE test.test SELECT t.* from (SELECT '%s','%s','%.4f','%.4f') as t" % (symbol,key,open,close))
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False

sqlContext.sql("SELECT * FROM test.consultants").show()
c.close()
