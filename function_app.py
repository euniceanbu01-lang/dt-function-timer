import azure.functions as func
import logging
import requests
import json

from predict import predict_leak
from prescribe import predict_leak as prescribe_predict

app = func.FunctionApp()

# ==========================
# ThingSpeak Configuration
# ==========================

CHANNEL_ID = "3149051"
READ_API_KEY = "NWKBNFL3252PISBY"

THINGSPEAK_URL = (
    f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds/last.json"
    f"?api_key={READ_API_KEY}"
)

# ==========================
# TIMER TRIGGER
# Every 1 Minute
# ==========================

@app.function_name(name="digitalTwinTimer")
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer")
def digitalTwinTimer(myTimer: func.TimerRequest) -> None:

    logging.info("Digital Twin Timer Trigger Started")

    try:
        response = requests.get(THINGSPEAK_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        timestamp = data.get("created_at")

        sensors = [
            ("Sensor_1", data.get("field1"), data.get("field2")),
            ("Sensor_2", data.get("field3"), data.get("field4")),
            ("Sensor_3", data.get("field5"), data.get("field6")),
        ]

        final_results = []

        for name, pressure, flow in sensors:

            if pressure is None or flow is None:
                continue

            pressure = float(pressure)
            flow = float(flow)

            prediction = predict_leak(pressure, flow)
            prescription = prescribe_predict(pressure, flow)

            sensor_result = {
                "sensor": name,
                "pressure_bar": pressure,
                "flow_lpm": flow,
                "prediction": prediction,
                "prescription": prescription
            }

            final_results.append(sensor_result)

        logging.info("===================================")
        logging.info(f"Timestamp: {timestamp}")
        logging.info(json.dumps(final_results, indent=2))
        logging.info("===================================")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
