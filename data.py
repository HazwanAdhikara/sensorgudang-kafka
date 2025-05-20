from kafka import KafkaProducer
import json
import time
import random
import threading

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudang_id = ['G1', 'G2', 'G3']

def kirim_suhu():
    while True:
        for gid in gudang_id:
            suhu = random.randint(70, 90)
            data = {"gudang_id": gid, "suhu": suhu}
            producer.send('sensor-suhu-gudang', value=data)
            print(f"[Suhu] Terkirim: {data}")
        time.sleep(1)

def kirim_kelembaban():
    while True:
        for gid in gudang_id:
            kelembaban = random.randint(60, 80)
            data = {"gudang_id": gid, "kelembaban": kelembaban}
            producer.send('sensor-kelembapan-gudang', value=data)
            print(f"[Kelembaban] Terkirim: {data}")
        time.sleep(1)

# Buat thread untuk masing-masing jenis data
threading.Thread(target=kirim_suhu).start()
threading.Thread(target=kirim_kelembaban).start()