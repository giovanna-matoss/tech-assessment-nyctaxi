from flask import Flask, Response
from flask_sse import sse
import csv
import time
import json

app = Flask(__name__)
app.config["REDIS_URL"] = "redis://redis:6379"
app.register_blueprint(sse, url_prefix='/stream')

def stream_events():
    with open('src/data/train.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sse.publish(json.dumps(row), type='taxi_event')
            time.sleep(5)

@app.route('/start_stream')
def start_stream():
    from threading import Thread
    thread = Thread(target=stream_events, daemon=True)
    thread.start()
    return "Streaming iniciado!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
