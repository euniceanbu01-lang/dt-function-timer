import logging
import os
import json
import datetime
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient

from predict import predict_leak
from prescribe import get_prescription

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer",
                   run_on_startup=False, use_monitor=True)
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

    processed_container = blob_service_client.get_container_client("digital-twin-processed")

    try:
        processed_container.create_container()
    except:
        pass

    response = requests.get(THINGSPEAK_URL)
    data = response.json()
    feed = data["feeds"][-1]

    timestamp = datetime.datetime.utcnow().isoformat()
    filename_time = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    # -------- 3 Sensor Pairs --------
    pressures = [
        float(feed.get("field1", 0)),
        float(feed.get("field3", 0)),
        float(feed.get("field5", 0))
    ]

    flows = [
        float(feed.get("field2", 0)),
        float(feed.get("field4", 0)),
        float(feed.get("field6", 0))
    ]

    sensors_output = []

    for i in range(3):

        prediction = predict_leak(pressures[i], flows[i])

        if prediction["leak"] == 1:
            prescription = get_prescription(
                prediction["leak_mm"],
                prediction["leak_lpm"]
            )
        else:
            prescription = {"message": "System normal"}

        sensors_output.append({
            "sensor_id": f"S{i+1}",
            "pressure": pressures[i],
            "flow": flows[i],
            "leak": prediction["leak"],
            "probability": prediction["prob"],
            "leak_lpm": prediction["leak_lpm"],
            "leak_mm": prediction["leak_mm"],
            "prescription": prescription
        })

    processed_output = {
        "timestamp": timestamp,
        "sensors": sensors_output
    }

    # Save latest file
    processed_container.upload_blob(
        "latest_prediction.json",
        json.dumps(processed_output),
        overwrite=True
    )

    logging.info("Digital Twin Execution Completed")
