from confluent_kafka import Producer
import random
import time
import json 

def generate_data():
    customer_id = 'SBI' + str(random.randint(1,10000))
    month = random.choice(['Jan','Feb','Mar',"Apr","May",'Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
    category = random.choice(['Electronics', 'Food', 'Entertainment', 'Bills', 'Travel', 'Others'])
    payment_type = random.choice(['Credit Card','Debit Card','Net Banking', 'UPI'])
    spend = random.randint(100,60000)
    transaction_id = random.randint(1000000000,9999999999)

    data = {
        "customer_id": customer_id,
        "month":month,
        "category":category,
        "payment_type":payment_type,
        "spend":spend,
        "transaction_id":transaction_id
    }
    return data

duration = 2
producer = Producer({
        'bootstrap.servers': "bootstrap_servers",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': "username",
        'sasl.password': "password"
})

start_time = time.time()
counter = 0
while time.time() - start_time < duration:
    data = generate_data()
    producer.produce("kafka_topic_name", key = str(data['transaction_id']), value = json.dumps(data))
    counter +=1
    print(data)

print('Total records generated and publish to topic', counter)
