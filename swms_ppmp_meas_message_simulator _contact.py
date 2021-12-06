import random
import time
from datetime import datetime

import paho.mqtt.client as mqtt

import json

PERIOD_IN_MS = 20

# mqtt_host = "HOST"
# user_name = "USERNAME"
# password = "PW"
# port = 8883
# keepalive = 60

mqtt_host = "windio-contact.northeurope.cloudapp.azure.com"
user_name = ""  # Add user
password = ""  # Add password
port = 1883
keepalive = 350


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))


client = mqtt.Client()
client._username = user_name
client._password = password
client.on_connect = on_connect
# client.tls_set()

client.connect(mqtt_host, port, 60)


def make_message():
    dimensions = {
        "temperature": "Celsius",
        "absolute_humidity": "Grams per kilogram",
        "turbine_speed": "ms",
    }

    data_points = [
        (random.randint(10, 50), random.randint(70, 100), random.randint(90, 130)),
        (random.randint(10, 50), random.randint(70, 100), random.randint(90, 130)),
        (random.randint(10, 50), random.randint(70, 100), random.randint(90, 130)),
    ]

    measurements_arr = []
    ts_utcnow = datetime.utcnow()
    ts_utcnow = ts_utcnow.strftime("%Y-%m-%dT%H:%M:%SZ")

    time_offsets = []
    for i, point in enumerate(data_points):
        time_offsets.append(i * PERIOD_IN_MS)

    for i, dim in enumerate(dimensions):
        values = []
        for point in data_points:
            values.append(point[i])

        meas = {
            "context": {dim: {"unit": dimensions[dim]}},
            "ts": ts_utcnow,
            "series": {"time": time_offsets, dim: values},
        }
        measurements_arr.append(meas)

    msg_json = {
        "content-spec": "urn:spec://eclipse.org/unide/measurement-message#v3",
        "device": {"id": user_name},
        "measurements": measurements_arr,
    }

    msg = json.dumps(msg_json, indent=2)
    return msg


while True:
    payload = make_message()
    print(payload)
    # client.publish("windio/iot/simulator", payload)
    client.loop()
    client.publish("ppmpv3/WT01/DDATA/urn:uni-bremen:bik:wio:1:1:wind:1234", payload)
    for i in range(5):
        print("Sekunden seit letzter Nachricht: " + str(i))
        time.sleep(1)


# client.loop_stop()
