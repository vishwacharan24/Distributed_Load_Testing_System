from flask import Flask, request, jsonify,render_template
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import time
from statistics import mean, median,mode
import threading
app = Flask(__name__)

class Orchestrator:
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.orchestrator_id = str(uuid.uuid4())
        self.active_tests = {}  # Dictionary to store information about active tests
        self.metrics_store = {}  # Dictionary to store metrics from driver nodes
        self.drivers_last_heartbeat = {}  # Dictionary to store the last heartbeat timestamp for each driver
        self.heartbeat_threshold = 10
        self.driv_count=0
        # Start the heartbeat consumer in a separate thread
        self.start_heartbeat_consumer()
        #self.print_reg()
        reg_thread=threading.Thread(target=self.print_reg)
        reg_thread.start()
    def print_reg(self):
        print("in")
        consumer=KafkaConsumer("register", bootstrap_servers=self.kafka_bootstrap_servers,
                                 value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        print(consumer)
        #self.driv_count+=1
        #print(self.driv_count)
        for message in consumer:
             self.driv_count+=1
             print(self.driv_count)
             print(f"Registration successfull:{message.value['driver_id']}")

    def start_heartbeat_consumer(self):
        # Start a separate thread to continuously listen for heartbeat messages
        # You may need to implement the actual logic for handling heartbeats
        def heartbeat_listener():
            consumer = KafkaConsumer("heartbeat", bootstrap_servers=self.kafka_bootstrap_servers,
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            for message in consumer:
                heartbeat_message = message.value
                self.handle_heartbeat(heartbeat_message)

        import threading
        heartbeat_thread = threading.Thread(target=heartbeat_listener)
        heartbeat_thread.start()

    """def handle_heartbeat(self, heartbeat_message):
        # Add logic here to handle heartbeat messages from driver nodes
        # For example, update the last seen timestamp for the corresponding driver node
        node_id = heartbeat_message.get("node_id")
        timestamp = heartbeat_message.get("timestamp")
        print(f"Heartbeat received from {node_id} at {timestamp}")"""

    def handle_heartbeat(self, heartbeat_message):
        # Add logic here to handle heartbeat messages from driver nodes
        node_id = heartbeat_message.get("node_id")
        timestamp = heartbeat_message.get("timestamp")
        
        # Check if this is the first heartbeat from the driver
        if node_id not in self.drivers_last_heartbeat:
            self.drivers_last_heartbeat[node_id] = timestamp
        else:
            # Check the time difference between successive heartbeats
            time_difference = timestamp - self.drivers_last_heartbeat[node_id]
            if time_difference > self.heartbeat_threshold:
                print(f"Heartbeat delay from {node_id} exceeds threshold: {time_difference} seconds")

            # Update the last seen timestamp for the corresponding driver node
            self.drivers_last_heartbeat[node_id] = timestamp

    def publish_test_config(self, test_type, test_message_delay, message_count_per_driver,num_drivers):
        test_id = str(uuid.uuid4())
        test_config_message = {
            "test_id": test_id,
            "test_type": test_type,
            "test_message_delay": test_message_delay,
            "message_count_per_driver": message_count_per_driver,
            "num_drivers":num_drivers
        }
        self.produce_message("test_config", test_config_message)
        return test_id

    def publish_trigger_message(self, test_id):
        trigger_message = {
            "test_id": test_id,
            "trigger": "YES"
        }
        self.produce_message("trigger", trigger_message)
        return f"Trigger message published for Test ID: {test_id}"

    def produce_message(self, topic, message):
        producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        try:
            producer.send(topic, json.dumps(message).encode("utf-8"))
            producer.flush()
            print(f"Produced message to topic {topic}: {message}")
        except Exception as e:
            print(f"Error producing message to topic {topic}: {e}")

    def receive_metrics(self):
        consumer = KafkaConsumer("metrics", bootstrap_servers=self.kafka_bootstrap_servers,
                                 value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        for message in consumer:
            metrics_message = message.value
            self.handle_metrics(metrics_message)

    def handle_metrics(self, metrics_message):
        # Add logic here to handle metrics messages from driver nodes
        # For example, store the metrics in the metrics_store dictionary
        test_id = metrics_message.get("test_id")
        node_id = metrics_message.get("node_id")
        report_id = metrics_message.get("report_id")
        metrics_data = metrics_message.get("metrics")

        if test_id not in self.metrics_store:
            self.metrics_store[test_id] = {}

        if node_id not in self.metrics_store[test_id]:
            self.metrics_store[test_id][node_id] = {}

        self.metrics_store[test_id][node_id][report_id] = metrics_data
        print(f"Metrics received for Test ID {test_id} from Node {node_id} with {metrics_data}")

    def start_load_test(self, test_type, test_message_delay, message_count_per_driver,num_drivers):
        test_id = self.publish_test_config(test_type, test_message_delay, message_count_per_driver,num_drivers)
        self.publish_trigger_message(test_id)
        self.receive_metrics()
        return test_id

    def get_test_status(self, test_id):
        # Add logic here to retrieve the status of the test with the given test_id
        # For example, you could check if the test is still running or completed
        # Replace the following line with the actual implementation
        if test_id in self.active_tests:
            return self.active_tests[test_id]["status"]
        else:
            return "Test not found"

    def stop_test(self, test_id):
        # Add logic here to stop the test with the given test_id
        # For example, you could send a message to driver nodes to stop the test
        # Replace the following line with the actual implementation
        if test_id in self.active_tests:
            # Implement logic to stop the test, if needed
            del self.active_tests[test_id]
            return f"Test with ID {test_id} stopped"
        else:
            return "Test not found"


@app.route('/loadtest', methods=['POST'])
def start_load_test():
    data = request.get_json()
    test_type = data.get("test_type")
    test_message_delay = data.get("test_message_delay")
    message_count_per_driver = data.get("message_count_per_driver")
    num_drivers=data.get("num_drivers")
    test_id = orchestrator.start_load_test(test_type, test_message_delay, message_count_per_driver,num_drivers)
    return jsonify({"test_id": test_id, "status": "Load test started"})

"""@app.route('/tests/<test_id>', methods=['GET'])
def get_test_status(test_id):
    status = orchestrator.get_test_status(test_id)
    return jsonify({"test_id": test_id, "status": status})

@app.route('/tests/<test_id>/stop', methods=['POST'])
def stop_test(test_id):
    result = orchestrator.stop_test(test_id)
    return jsonify({"test_id": test_id, "status": result})"""
@app.route('/dashboard/<test_id>', methods=['GET'])
def show_dashboard(test_id):
    if test_id not in orchestrator.metrics_store:
        return jsonify({"test_id": test_id, "status": "Test not found"})

    metrics_data = orchestrator.metrics_store[test_id]
    return render_template('dashboard.html', test_id=test_id, metrics_data=metrics_data)
if __name__ == "__main__":
    orchestrator = Orchestrator("localhost:9092")
    app.run(port=5000)
