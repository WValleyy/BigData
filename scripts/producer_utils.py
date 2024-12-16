import json
import time

def send_to_kafka(producer, kafka_topic, key, value):
    try:
        producer.send(kafka_topic, key=key.encode('utf-8'), value=json.dumps(value).encode('utf-8'))
        producer.flush()
        print(f"Sent to Kafka: {value}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

import json
import time
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
def send_to_kafka(producer, topic, key, message):
    # Sending a message to Kafka
    # producer.produce(topic, key=key, partition=0, value=json.dumps(message).encode("utf-8"))
    producer.produce(topic, key=key, value=json.dumps(message).encode("utf-8"), callback=delivery_report)
    producer.flush()

def read_data_from_json(file_path):
    """Generator function to read JSON file line by line."""
    try:
       with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                yield json.loads(line)  # Assuming each line is a valid JSON record.
    except Exception as e:
        print(f"Error reading JSON file: {e}")

def ingest_send_data(producer, kafka_topic, file_path, trunk_size=10):
    """Reads data from a JSON file line by line, accumulates in trunks, and sends to Kafka."""
    trunk = []
    for index, record in enumerate(read_data_from_json(file_path)):
        key = record.get('review_id', f"key-{index}")  # Use 'review_id' as the Kafka message key if available
        trunk.append(record)
        
        # When trunk reaches the specified size, send to Kafka and clear trunk
        if len(trunk) >= trunk_size:
            for item in trunk:
                send_to_kafka(producer, kafka_topic, key, item)
            trunk.clear()  # Clear trunk after sending
            
        time.sleep(1)  # Sleep to simulate a stream

    # Send any remaining records in trunk
    if trunk:
        for item in trunk:
            send_to_kafka(producer, kafka_topic, key, item)



