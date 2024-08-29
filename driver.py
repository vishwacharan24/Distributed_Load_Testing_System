import time
import json
import requests
from kafka import KafkaProducer, KafkaConsumer
from statistics import mean, median,mode
from flask import Flask, jsonify
import uuid
import threading

app = Flask(__name__)

class DriverNode:
    def __init__(self, driver_id,kafka_bootstrap_servers, target_server_url, orchestrator_url):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.target_server_url = target_server_url
        self.orchestrator_url = orchestrator_url
        self.driver_id = driver_id
        self.response_times = []
        self.terminate_event = threading.Event()  # Event to signal termination

        # Start a separate thread to continuously send heartbeats
        self.start_heartbeat_sender()

    def register_with_orchestrator(self):
        registration_message = {
            "driver_id": self.driver_id,
            "driver_ip": f"{self.orchestrator_url}:5000"  # Adjust the port as needed
        }
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        producer.send("register", value=json.dumps(registration_message).encode("utf-8"))
        producer.flush()
        print("Registration message sent to orchestrator.")

    def start(self, num_requests):
        self.register_with_orchestrator()
        consumer = KafkaConsumer("test_config", "trigger", bootstrap_servers=self.kafka_bootstrap_servers,
                                  auto_offset_reset='latest',
                                 value_deserializer=lambda v: json.loads(v.decode('utf-8')))

        test_config = None
        for message in consumer:
            if message.topic == "test_config":
                test_config = message.value
                print(f"Received test configuration: {test_config}")

            if message.topic == "trigger" and test_config:
                trigger_message = message.value
                if trigger_message.get("trigger") == "YES":
                    print("Received trigger message. Starting load test.")
                    self.perform_load_test(test_config, num_requests)
                    break

        # Signal termination to the heartbeat thread
        self.terminate_event.set()
        return "Load test completed."

    def perform_load_test(self, test_config, num_requests):
        test_type = test_config["test_type"]
        if test_type == "AVALANCHE":
            self.avalanche_test(test_config, num_requests)
        elif test_type == "TSUNAMI":
            delay_interval = test_config["test_message_delay"]
            self.tsunami_test(test_config, delay_interval, num_requests)

    def avalanche_test(self, test_config, num_requests):
        for i in range(num_requests):
            if self.terminate_event.is_set():
                break  # Exit the loop if termination is signaled
            response_time = self.send_request()
            self.record_statistics(response_time)
        print("record_statistics", self.response_times)
        self.publish_metrics(test_config)

    def tsunami_test(self, test_config, delay_interval, num_requests):
        request_interval = delay_interval / 1000  # Convert delay_interval to seconds
        for i in range(num_requests):
            if self.terminate_event.is_set():
                break  # Exit the loop if termination is signaled
            response_time = self.send_request()
            self.record_statistics(response_time)
            time.sleep(request_interval)
        print("record_statistics", self.response_times)
        self.publish_metrics(test_config)

    def send_request(self):
        start_time = time.time()
        response = requests.get(self.target_server_url)
        time.sleep(0.1)
        end_time = time.time()
        response_time = (end_time - start_time) * 1000
        print("response_time", response_time)
        return response_time

    def record_statistics(self, response_time):
        self.response_times.append(response_time)

    def publish_metrics(self, test_config):
        metrics_message = {
            "node_id": self.driver_id,
            "test_id": test_config["test_id"],
            "report_id": str(uuid.uuid4()),
            "metrics": {
                "mean_latency": mean(self.response_times),
                "median_latency": median(self.response_times),
                "mode_latency": mode(self.response_times),
                "min_latency": min(self.response_times),
                "max_latency": max(self.response_times)
            }
        }
        print(num_requests)
        print("Metrics:", json.dumps(metrics_message))
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        producer.send("metrics", value=json.dumps(metrics_message).encode("utf-8"))
        producer.flush()

    def start_heartbeat_sender(self):
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.start()

    def send_heartbeat(self):
        while not self.terminate_event.is_set():
            heartbeat_message = {
                "node_id": self.driver_id,
                "heartbeat": "YES",
                "timestamp": time.time()
            }
            producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
            producer.send("heartbeat", value=json.dumps(heartbeat_message).encode("utf-8"))
            producer.flush()
            print(f"Heartbeat sent from Node {self.driver_id}")
            time.sleep(5)  # Adjust the interval as needed

if __name__ == "__main__":
    kafka_ip = "localhost:9092"
    target_server_url = "http://localhost:8000/ping"
    orchestrator_url = "http://localhost:5000"  # Replace with the actual Orchestrator URL
    num_requests = 4
    num_drivers=3
    # Instantiate the DriverNode
    driver_threads=[]
    for i in range(num_drivers):
        driver_node = DriverNode(driver_id=f"Driver-{i+1}",kafka_bootstrap_servers=kafka_ip, target_server_url=target_server_url,
                             orchestrator_url=orchestrator_url)
        start_thread = threading.Thread(target=driver_node.start, args=(num_requests,))
        driver_threads.append(start_thread)
        start_thread.start()
    for t in driver_threads:
        t.join()  # Wait for the thread to complete
    print("Driver process terminated.")
