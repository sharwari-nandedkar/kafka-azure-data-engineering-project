import random
import time 
import json
from confluent_kafka import Producer

def generate_data():
    customer_id = 'SBI' + str(random.randint(1,10000))
    month = random.choice(["Jan", "Feb", "March", "April", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"])
    category = random.choice(["Electronics", "Food", "Entertainment", "Bills", "Travels", "Others"])
    payment_type = random.choice(["Credit card", "Debit Card", "Net Banking", "UPI"])
    spend = random.randint(100,60000)
    transaction_id = random.randint(1000000000,9999999999)
    data = {
        "customer_id":customer_id,
        "month":month,
        "category":category,
        "payment_type":payment_type,
        "spend":spend,
        "transaction_id":transaction_id
    }
    return data

duration = 2
producer = Producer({
    'bootstrap.servers': "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': "3XKIVHPOESDXSEK6",
    'sasl.password': "zkModcGVi3JhJeRTO4o1cXJj+6o9OPwcnoqX713q42lNq9VMHvAjY+4BY6biCGrj"
})

start_time = time.time()
count = 0
while time.time() - start_time < duration:
    data = generate_data()
    producer.produce("topic_0", key = str(data['transaction_id']), value = json.dumps(data))
    count += 1
    print(data)
    
producer.flush()
print("no of records generated:",count)
