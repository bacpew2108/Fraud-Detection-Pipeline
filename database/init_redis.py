import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import redis
import json
import random
from faker import Faker
from config import REDIS_HOST, REDIS_PORT

fake = Faker()

redis_client = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0, decode_responses=True)

def generate_and_load_customers(num_customers=10000):
    print(f"Bắt đầu tạo và nạp {num_customers} hồ sơ khách hàng vào Redis...")
    pipe = redis_client.pipeline()
    
    risk_segments = ["Low", "Medium", "High"]
    
    for i in range(1, num_customers + 1):
        account_id = f"ACC_{i:05d}"
        
        customer_data = {
            "age": random.randint(18, 70),
            "home_city": fake.city(),
            "risk_segment": random.choices(risk_segments, weights=[0.7, 0.2, 0.1])[0] # Phân bổ 70% Low, 20% Medium, 10% High
        }
        
        # Lệnh nạp vào Redis: Key là account_id, Value là chuỗi JSON
        pipe.set(account_id, json.dumps(customer_data))
        
        # Cứ gom đủ 2,000 records thì mới thực thi (execute) đẩy vào Redis một lần
        if i % 2000 == 0:
            pipe.execute()
            print(f"Đã nạp thành công {i} records...")
            
    # Thực thi đẩy nốt những records còn dư cuối cùng
    pipe.execute()
    print("Hoàn tất quá trình nạp dữ liệu tĩnh (Lookup Cache) vào Redis!")

if __name__ == "__main__":

    redis_client.flushdb() 
    generate_and_load_customers(num_customers=10000)