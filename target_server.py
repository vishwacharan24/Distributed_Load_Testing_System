from flask import Flask, jsonify

app = Flask(__name__)

class TargetServer:
    def __init__(self):
        self.request_count = 0
        self.response_count = 0

    def increment_request_count(self):
        self.request_count += 1

    def increment_response_count(self):
        self.response_count += 1

target_server = TargetServer()

@app.route('/ping', methods=['GET'])
def ping():
    target_server.increment_request_count()
    target_server.increment_response_count()
    return jsonify({"message": "pong"})

@app.route('/metrics', methods=['GET'])
def metrics():
    return jsonify({
        "request_count": target_server.request_count,
        "response_count": target_server.response_count
    })

if __name__ == '__main__':
    app.run(port=8000)
