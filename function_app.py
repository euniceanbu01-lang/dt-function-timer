import logging
import azure.functions as func
import requests
import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

from predict import predict_leak
import pandas as pd

# =========================
# CONFIGURATION
# =========================

THINGSPEAK_CHANNEL_ID = "3149051"
THINGSPEAK_READ_KEY = "NWKBNFL3252PISBY"

RAW_CONTAINER = "digital-twin-raw"
PROCESSED_CONTAINER = "digital-twin-processed"

# =========================
# AZURE FUNCTION APP INIT
# =========================

app = func.FunctionApp()

# =========================
# TIMER TRIGGER (Every 1 Minute)
# =========================

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False)
def digitalTwinTimer(myTimer: func.TimerRequest) -> None:

    logging.info("Digital Twin Timer Triggered")

    try:

        # =========================
        # 1️⃣ FETCH THINGSPEAK DATA
        # =========================

        url = f"https://api.thingspeak.com/channels/{THINGSPEAK_CHANNEL_ID}/feeds.json?api_key={THINGSPEAK_READ_KEY}&results=1"
        response = requests.get(url)
        data = response.json()

        feed = data["feeds"][0]

        timestamp = feed["created_at"]

        sensors = {
            "Pressure_1": float(feed["field1"] or 0),
            "Flow_Rate_1": float(feed["field2"] or 0),
            "Pressure_2": float(feed["field3"] or 0),
            "Flow_Rate_2": float(feed["field4"] or 0),
            "Pressure_3": float(feed["field5"] or 0),
            "Flow_Rate_3": float(feed["field6"] or 0),
        }

        # =========================
        # 2️⃣ PREDICTION LAYER
        # =========================

        prediction_results = {}

        for i in range(1, 4):

            p = sensors[f"Pressure_{i}"]
            f = sensors[f"Flow_Rate_{i}"]

            result = predict_leak(p, f)

            prediction_results[f"Sensor_{i}"] = result

        # =========================
        # 3️⃣ PRESCRIPTION LAYER
        # =========================

        prescription_df = pd.read_csv("prescription.csv")

        prescriptions = {}

        for key, value in prediction_results.items():

            if value["leak"] == 1:

                leak_mm = value["leak_mm"]

                match = prescription_df[
                    (prescription_df["min_mm"] <= leak_mm) &
                    (prescription_df["max_mm"] >= leak_mm)
                ]

                if not match.empty:
                    prescriptions[key] = match.iloc[0]["action"]
                else:
                    prescriptions[key] = "Inspect manually"

            else:
                prescriptions[key] = "No action required"

        # =========================
        # 4️⃣ COMBINE OUTPUT
        # =========================

        final_output = {
            "timestamp": timestamp,
            "sensor_readings": sensors,
            "prediction": prediction_results,
            "prescription": prescriptions
        }

        # =========================
        # 5️⃣ STORE TO BLOB
        # =========================

        connect_str = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        raw_container_client = blob_service_client.get_container_client(RAW_CONTAINER)
        processed_container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)

        raw_container_client.upload_blob(
            name=f"raw_{datetime.utcnow().isoformat()}.json",
            data=json.dumps(sensors),
            overwrite=True
        )

        processed_container_client.upload_blob(
            name="latest_prediction.json",
            data=json.dumps(final_output),
            overwrite=True
        )

        logging.info("Digital Twin Execution Completed Successfully")

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
