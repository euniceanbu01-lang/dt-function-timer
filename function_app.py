import logging
import os
import json
import datetime
import requests
import pandas as pd
import joblib
import azure.functions as func
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True)
def digitalTwinTimer(myTimer: func.TimerRequest) -> None:

    logging.info("Digital Twin Timer Triggered")

    CHANNEL_ID = os.getenv("CHANNEL_ID")
    READ_API_KEY = os.getenv("READ_API_KEY")
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    THINGSPEAK_URL = (
        f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json"
        f"?api_key={READ_API_KEY}&results=1"
    )

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    raw_container = blob_service_client.get_container_client("digital-twin-raw")
    processed_container = blob_service_client.get_container_client("digital-twin-processed")

    try:
        raw_container.create_container()
    except:
        pass

    try:
        processed_container.create_container()
    except:
        pass

    response = requests.get(THINGSPEAK_URL)
    data = response.json()
    feed = data["feeds"][-1]

    bundle = joblib.load("model.joblib")
    model = bundle["model"]
    threshold = bundle["threshold"]

    timestamp = datetime.datetime.utcnow().isoformat()
    filename_time = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    pressure = float(feed.get("field1", 45))
    flow = float(feed.get("field2", 100))

    raw_output = {
        "timestamp": timestamp,
        "pressure": pressure,
        "flow": flow
    }

    pressure_psi = pressure * 14.5038
    flow_gpm = flow / 3.78541

    X = pd.DataFrame([[pressure_psi, flow_gpm]],
                     columns=["Pressure", "Flow_Rate"])

    prob = model.predict_proba(X)[0][1]
    leak = int(prob >= threshold)

    processed_output = {
        "timestamp": timestamp,
        "leak": leak,
        "probability": float(prob)
    }

    raw_container.upload_blob(
        f"{filename_time}_raw.json",
        json.dumps(raw_output),
        overwrite=True
    )

    processed_container.upload_blob(
        f"{filename_time}_processed.json",
        json.dumps(processed_output),
        overwrite=True
    )

    logging.info("Digital Twin Execution Completed")
