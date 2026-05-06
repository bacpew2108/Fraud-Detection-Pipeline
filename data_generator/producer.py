import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from faker import Faker
from kafka import KafkaProducer
import json
import time
import random
from config import KAFKA_BOOTSTRAP_SERVERS

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

messages_count = 0

try:
    while True:
        idOrig = f"ACC_{random.randint(1, 10000):05d}"
    
        dest_prefix = random.choice(['C', 'M'])
        idDest = f"{dest_prefix}_{random.randint(1, 50000):05d}"
        
        type_tx = random.choice(['TRANSFER', 'CASH_OUT', 'PAYMENT', 'DEBIT'])
        
        oldbalanceOrg = round(random.uniform(1000.0, 50000.0), 2)
        
        is_fraud_attempt = random.random() < 0.05 
        
        if is_fraud_attempt:
            amount = oldbalanceOrg  
        else:
            amount = round(random.uniform(10.0, oldbalanceOrg * 0.3), 2) 

        newbalanceOrig = round(oldbalanceOrg - amount, 2)
        
        oldbalanceDest = round(random.uniform(0.0, 20000.0), 2) if dest_prefix == 'C' else 0.0
        newbalanceDest = round(oldbalanceDest + amount, 2) if dest_prefix == 'C' else 0.0

        transaction = {
            "transaction_id": fake.numerify('#########'),
            "timestamp": fake.iso8601(),
            "type": type_tx,
            "amount": amount,
            "idOrig": idOrig,
            "oldbalanceOrg": oldbalanceOrg,
            "newbalanceOrig": newbalanceOrig,
            "idDest": idDest,
            "oldbalanceDest": oldbalanceDest,
            "newbalanceDest": newbalanceDest
        }

        producer.send('transactions', transaction)
        messages_count += 1
        
        if messages_count % 500 == 0:
            print(f"Đã đẩy {messages_count} GD. Mẫu: {idOrig} -> {idDest} | Tiền: ${amount} | Bị rút sạch: {is_fraud_attempt}")
        
        time.sleep(0.002) 

except KeyboardInterrupt:
    producer.flush()
    producer.close()