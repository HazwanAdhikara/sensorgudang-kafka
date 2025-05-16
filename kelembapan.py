import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
gudang = ['G1','G2','G3']
while True:
    for g in gudang:
        data = {'gudang_id': g, 'kelembaban': random.randint(60, 85)}
        producer.send('sensor-kelembaban-gudang', data)
    producer.flush()
    time.sleep(1)