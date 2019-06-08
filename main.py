import requests
import os
import pika
import json
import json_logging
import psycopg2 as pg
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


def connect_db():
    try:
        pghost = Utils.get_secret("pg_host")
        pguser = Utils.get_secret("pg_user")
        pgpass = Utils.get_secret("pg_pass")
        conn = pg.connect(host=pghost, user=pguser, password=pgpass, port="5432", database="cementos")
        return conn
    except (Exception, pg.Error) as error:
        logger.error("Can't connect to postgres", extra={'error': {"raw": error, "app": config["name"],
                                                                   "label": config["name"]}})
        return None


def fix_data(msg):
    data = json.loads(msg)
    for event in data["events"]:
        camevent = dict()
        if "gpsLocationStampModule" in event:
            camevent["course"] = event["gpsLocationStampModule"]["course"]
        else:
            camevent["course"] = 0
        camevent["latitude"] = event["header"]["Latitude"]
        camevent["longitude"] = event["header"]["Longitude"]
        camevent["UnitId"] = event["header"]["UnitId"]
        camevent["groundSpeed"] = event["header"]["Speed"]
        camevent["utcTimestampSeconds"] = event["header"]["UtcTimestampSeconds"]
        camevent["Odometer"] = event["header"]["Odometer"]
        camevent["eventType"] = event["header"]["TemplateId"]
        if camevent["eventType"] == 132:
            camevent["pumping"] = -1
        elif camevent["eventType"] == 133:
            camevent["pumping"] = 1
        else:
            camevent["pumping"] = 0
        camevent["client"] = "Cementos"
        if "variablesDumpListModule" in event:
            variables = event["variablesDumpListModule"]["variables"]
            camevent["variables"] = json.dumps(variables)
            for vari in variables:
                if vari["title"] == "Engine Speed":
                    camevent["engineSpeed"] = vari["resultValue"]
                elif vari["title"] == "Fuel Level":
                    camevent["fuelLevel"] = vari["resultValue"]
                elif vari["title"] == "Engine Total Fuel Used":
                    camevent["totalUsedFuel"] = vari["resultValue"]
                elif vari["title"] == "Engine Fuel Rate":
                    camevent["fuelRate"] = vari["resultValue"]
        else:
            camevent["variables"] = ""
            camevent["engineSpeed"] = 0
            camevent["fuelRate"] = 0
            camevent["totalUsedFuel"] = 0
            camevent["fuelLevel"] = 0

        resp = requests.post(url+"/api/canevents", json=camevent)
        print(resp)


def insert_string(event):
    fuelLevel = 0
    totalUsedFuel = 0
    fuelRate = 0
    if "variablesDumpListModule" in event:
        variables = event["variablesDumpListModule"]["variables"]
        for vari in variables:
            if vari["title"] == "Fuel Level":
                fuelLevel = vari["resultValue"]
            elif vari["title"] == "Engine Total Fuel Used":
                totalUsedFuel = vari["resultValue"]
            elif vari["title"] == "Engine Fuel Rate":
                fuelRate = vari["resultValue"]

    sql = 'INSERT INTO canevents("UnitId",latitude,longitude,"groundSpeed","fuelLevel",' \
          '"fuelRate","totalUsedFuel","eventType","utcTimestampSeconds")'\
          ' values('+str(event["header"]["UnitId"])+','+str(event["header"]["Latitude"])+'' \
          ','+str(event["header"]["Longitude"])+','+str(event["header"]["Speed"])+'' \
          ','+str(fuelLevel)+','+str(fuelRate)+','+str(totalUsedFuel)+','+str(event["header"]["TemplateId"])+'' \
          ','++')'
    return sql


def insert_db(msg):
    connection = connect_db()
    with connection:
        with connection.cursor() as cursor:
            data = json.loads(msg)
            for event in data["events"]:
                sql = insert_string(event)
                logger.info("Inserting event",
                            extra={'props': {"raw": sql, "app": config["name"], "label": config["name"]}})
                cursor.execute(sql)
            cursor.commit()
        connection.close()


def callback(ch, method, properties, body):
    logger.info("Reading message", extra={'props': {"raw": body, "app": config["name"], "label": config["name"]}})
    insert_db(body)


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
