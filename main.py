import requests
import os
import pika
import json
import json_logging
import logging
import sys
from PydoNovosoft.utils import Utils


json_logging.ENABLE_JSON_LOGGING = True
json_logging.init()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

config = Utils.read_config("package.json")
env_cfg = config[os.environ["environment"]]

url = env_cfg["API_URL"]
rabbitmq = env_cfg["RABBITMQ_URL"]

if env_cfg["secrets"]:
    rabbit_user = Utils.get_secret("rabbitmq_user")
    rabbit_pass = Utils.get_secret("rabbitmq_passw")
else:
    rabbit_user = env_cfg["rabbitmq_user"]
    rabbit_pass = env_cfg["rabbitmq_passw"]


def fix_data(msg):
    data = json.loads(msg)
    for event in data["events"]:
        camevent = dict()
        camevent["course"] = event["gpsLocationStampModule"]["course"]
        camevent["latitude"] = event["gpsTimeStampModule"]["header"]["Latitude"]
        camevent["longitude"] = event["gpsTimeStampModule"]["header"]["Longitude"]
        camevent["UnitId"] = event["gpsTimeStampModule"]["header"]["UnitId"]
        camevent["groundSpeed"] = event["gpsTimeStampModule"]["header"]["Speed"]
        camevent["utcTimestampSeconds"] = event["gpsTimeStampModule"]["header"]["UtcTimestampSeconds"]
        camevent["Odometer"] = event["gpsTimeStampModule"]["header"]["Odometer"]
        camevent["client"] = "Cementos"
        variables = event["gpsTimeStampModule"]["header"]["variablesDumpListModule"]["variables"]
        camevent["variables"] = variables
        for vari in variables:
            if vari["title"] == "Engine Speed":
                camevent["engineSpeed"] = vari["resultValue"]
            elif vari["title"] == "Fuel Level":
                camevent["fuelLevel"] = vari["resultValue"]
            elif vari["title"] == "Engine Total Fuel Used":
                camevent["totalUsedFuel"] = vari["resultValue"]
            elif vari["title"] == "Engine Fuel Rate":
                camevent["fuelRate"] = vari["resultValue"]
        resp = requests.post(url+"/api/camevents", json=camevent)
        print(resp)


def callback(ch, method, properties, body):
    logger.info("Reading message", extra={'props': {"raw": body, "app": config["name"], "label": config["name"]}})
    fix_data(body)


def start():
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(rabbitmq, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(config["queue"], durable=True)
    channel.basic_consume(callback, config["queue"], no_ack=True)
    logger.info("Connection successful to RabbitMQ", extra={'props': {"app": config["name"], "label": config["name"]}})
    channel.start_consuming()


def main():
    print(Utils.print_title("package.json"))
    start()


if __name__ == '__main__':
    main()
