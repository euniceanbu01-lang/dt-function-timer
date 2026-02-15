import logging
import azure.functions as func
import requests
import os
import pandas as pd
from datetime import datetime

from predict import predict_leak

app = func.FunctionApp()

# ----------------------------
# ThingSpeak Configuration
# ----------------------------
CHANNEL_ID = "3149051"
READ_API_KEY = "NWKBNFL3252PISBY"

THINGSPEAK_URL = f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds/last.json?api_key={READ_API_KEY}"

# ----------------------------
# Load Prescription Table
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PRESCRIPTION_PATH = os.path.join(BASE_DIR, "prescription.csv")

prescription_df = pd.read_csv(PRESCRIPTION_PATH)


def get_prescription(leak_mm):
    """
    Match leak size to prescription table
    WITHOUT changing threshold logic.
    """
    if leak_mm == 0:
        return "No action required"

    # Assuming prescription.csv has columns like:
    # min_mm, max_mm, action
    for _, row in prescription_df.iterrows():
        if row["min_mm"] <= leak_mm <= row["max_mm"]:
            return row["action"]

    return "Inspect manually"


@app.route(route="run-digital-twin", methods=["GET"])
def run_digital_twin(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Digital Twin HTTP trigger started")

    try:
        # ----------------------------
        # 1. Fetch Latest Sensor Data
        # ----------------------------
        response = requests.get(THINGSPEAK_URL)
        data = response.json()

        timestamp = data["created_at"]

        sensors = [
            {
                "name": "Sensor_1",
                "pressure": float(data["field1"]),
                "flow": float(data["field2"])
            },
            {
                "name": "Sensor_2",
                "pressure": float(data["field3"]),
                "flow": float(data["field4"])
            },
            {
                "name": "Sensor_3",
                "pressure": float(data["field5"]),
                "flow": float(data["field6"])
            }
        ]

        results = []

        # ----------------------------
        # 2. Run Prediction + Prescription
        # ----------------------------
        for sensor in sensors:

            prediction = predict_leak(
                sensor["pressure"],
                sensor["flow"]
            )

            prescription = get_prescription(prediction["leak_mm"])

            results.append({
                "sensor": sensor["name"],
                "pressure_bar": sensor["pressure"],
                "flow_lpm": sensor["flow"],
                "prediction": prediction,
                "prescription": prescription
            })

        output = {
            "timestamp": timestamp,
            "channel_id": CHANNEL_ID,
            "results": results
        }

        logging.info(f"Output generated: {output}")

        return func.HttpResponse(
            body=pd.Series(output).to_json(),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )

