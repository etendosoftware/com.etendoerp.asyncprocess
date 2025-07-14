from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100000):
    message = {
         "id": "0",
         'asyncProcessId': f'process_{i}',
         "log": "0",
         'description': 'Stress test message',
         'params': "{   \"test\": 1, \"description\": \"this is and alert message 2\", \"jobs_jobs_id\": \"2B14362F797E4A36A1D9866E9960C11D\" }"
    }
    res = producer.send('TestJob.10', value=message)
    if i % 100 == 0:
        print(f'Sent message {i}')
        time.sleep(0.1)  # Controlar la tasa de envío
