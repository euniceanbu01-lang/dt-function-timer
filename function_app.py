import azure.functions as func
import logging
import requests
from datetime import datetime

from predict import predict_leak
from prescribe import predict_leak as prescribe_predict  # using same logic

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
# TIMER TRIGGER (Every 1 Min)
# ==========================
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False)
def digitalTwinTimer(myTimer: func.TimerRequest) -> None:

    logging.info("Digital Twin Timer Started")

    try:
        response = requests.get(THINGSPEAK_URL)
        data = response.json()

        timestamp = data.get("created_at", datetime.utcnow().isoformat())

        # ==========================
        # Read 3 Sensor Pairs
        # ==========================
        sensors = [
            ("Pressure_1", "Flow_Rate_1", data.get("field1"), data.get("field2")),
            ("Pressure_2", "Flow_Rate_2", data.get("field3"), data.get("field4")),
            ("Pressure_3", "Flow_Rate_3", data.get("field5"), data.get("field6")),
        ]

        results = []

        for name_p, name_f, pressure, flow in sensors:

            if pressure is None or flow is None:
                continue

            pressure = float(pressure)
            flow = float(flow)

            # ==========================
            # Prediction Layer
            # ==========================
            prediction = predict_leak(pressure, flow)

            # ==========================
            # Prescription Layer
            # (Using same threshold from model bundle)
            # ==========================
            prescription = prescribe_predict(pressure, flow)

            result = {
                "timestamp": timestamp,
                "sensor_pressure": name_p,
                "sensor_flow": name_f,
                "pressure_bar": pressure,
                "flow_lpm": flow,
                "leak": prediction["leak"],
                "probability": prediction["prob"],
                "leak_lpm": prediction["leak_lpm"],
                "leak_mm": prediction["leak_mm"],
                "prescription_leak": prescription["leak"]
            }

            results.append(result)

        # ==========================
        # Log Output (For Azure Logs & App Insights)
        # ==========================
        for r in results:
            logging.info(f"""
==============================
Timestamp: {r['timestamp']}
Sensor: {r['sensor_pressure']} & {r['sensor_flow']}
Pressure (bar): {r['pressure_bar']}
Flow (LPM): {r['flow_lpm']}
Leak Detected: {r['leak']}
Probability: {r['probability']}
Leak Size (mm): {r['leak_mm']}
Leak Volume (LPM): {r['leak_lpm']}
Prescription Flag: {r['prescription_leak']}
==============================
""")

        logging.info("Digital Twin Cycle Completed Successfully")

    except Exception as e:
        logging.error(f"Error in Digital Twin Timer: {str(e)}")

