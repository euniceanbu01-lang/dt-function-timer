import logging
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 */1 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def digitalTwinTimer(myTimer: func.TimerRequest) -> None:
    logging.info("Timer trigger is working properly.")
