# Basic Libraries
import datetime
import logging
from warnings import filterwarnings

# MQTT Library
import paho.mqtt.client as mqttClient

# Custom modules
import psql_func
import settings

filterwarnings('ignore')

# Logging
logging.basicConfig(
    filename=settings.STATUS_UPDATE_LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s %(message)s",
    level=logging.INFO,
)

# Topic
SUB_TOPIC = settings.UPDATE_SUB_TOPIC

# MQTT Credentials
BROKER_ADDRESS = settings.BROKER_ADDRESS
MQTT_PORT = settings.MQTT_PORT

USER = settings.MQTT_USER2
PASSWORD = settings.MQTT_PASSWORD2

# Connection state variable
Connected = False

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to broker")
        global Connected
        Connected = True

    else:
        logging.error("Connection failed an update from the status")


def on_disconnect(client, userdata, rc):
    if rc == 0:
        global Connected
        Connected = False
        logging.info("Disconnected")
    else:
        logging.error("Still running on the status")


def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))

    warehouse_id = msg.split(",")[0].strip()
    device_id = msg.split(",")[1].strip()

    # Updates the new status value and sends feedback to device
    flipped_status = psql_func.flip_status(warehouse_id, device_id)
    logging.info('Flipping status for %s/%s ' % (warehouse_id, device_id))
    client.publish(f"/{warehouse_id}/{device_id}", flipped_status)


def create_client():
    """Creates MQTT Client and assigns callbacks

    Returns:
        mqttClient: MQTT Clients
    """
    # Create client instance
    mqtt_client = mqttClient.Client()
    mqtt_client.username_pw_set(USER, password=PASSWORD)

    # Callbacks
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect

    return client


if __name__ == "__main__":

    # Create client
    client = create_client()

    # Connect to broker
    client.connect(BROKER_ADDRESS, port=MQTT_PORT)
    logging.info(f"Connected via Script ({USER})")

    # Subscribe to topic
    client.subscribe(SUB_TOPIC)

    # Loop forever
    client.loop_forever()
