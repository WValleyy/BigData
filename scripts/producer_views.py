import sys, socket
from pathlib import Path
path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))

from utils import load_environment_variables
from confluent_kafka import Producer
from dotenv import load_dotenv
from producer_utils import ingest_send_data
load_dotenv()
env_vars = load_environment_variables()
topic = "reviews_topic"
# Configuration for Kafka Producer
conf = {
    # Pointing to all three brokers
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)
path = r"G:\g\Hoc\2024.1.soict\Final_BigData\BigDataProject\BigDataProject2\src\datasets\yelp_academic_dataset_review.json"
if __name__ == '__main__':
    ingest_send_data(producer,
                    topic,
                    path
                            )
    
