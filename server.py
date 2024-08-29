from flask import Flask, render_template, request, redirect, url_for
import subprocess

app = Flask(__name__)

# Dictionary to store test details
test_details = {}


@app.route("/")
def home():
    return redirect(url_for("load_test"))


@app.route('/target')
def target():
    return "target server"


@app.route('/load_test')
def load_test():
    return render_template("load_test.html")


@app.route('/submit', methods=['POST'])
def submit():
    if request.method == 'POST':
        # Get form data
        request_count = request.form['requestCount']
        numberOfDrivers = request.form['numberOfDrivers']
        test_type = request.form['testType']
        if test_type == "Avalanche":
            message_delay = 0
        else:
            message_delay = request.form['messageDelay']
        message_count = request.form['messageCount']

        # Store details in dictionary
        test_details['request_count'] = request_count
        test_details['numberOfDrivers'] = numberOfDrivers
        test_details['test_type'] = test_type
        test_details['message_delay'] = message_delay
        test_details['message_count'] = message_count

        # Generate the shell script content
        script_content = f"""#!/bin/bash

# Command 1
curl -X POST -H "Content-Type: application/json" -d '{{"test_type": "{test_type.upper()}", "test_message_delay": {int(message_delay)}, "message_count_per_driver": {int(message_count)}}}' http://127.0.0.1:5000/loadtest

# Command 2
curl -X POST -H "Content-Type: application/json" -d '{{"num_drivers": {int(numberOfDrivers)}, "num_requests": {int(request_count)}}}' http://127.0.0.1:5001/loadtest
"""

        # Save the script content to a file
        script_path = "curl_script.sh"
        with open(script_path, 'w') as script_file:
            script_file.write(script_content)

        # Make the script executable
        subprocess.run(["chmod", "+x", script_path])

        # Execute the script
        subprocess.run(["./" + script_path])

    return render_template("result.html")


if __name__ == "__main__":
    app.run("127.0.0.1", 8080)
