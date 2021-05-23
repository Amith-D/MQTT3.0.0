# Basic libraries
import logging
import threading
import time
import pickle
from warnings import filterwarnings

# MQTT Library
import paho.mqtt.client as mqttClient

# Custom modules
import calculations, psql_func, settings
filterwarnings("ignore")

# Logging
logging.basicConfig(
    filename=settings.MQTT_LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s %(message)s",
    level=logging.INFO,
)

# Topics
SUB_TOPIC = settings.SUB_TOPIC

# MQTT Credentials
USER = settings.MQTT_USER
PASSWORD = settings.MQTT_PASSWORD

BROKER_ADDRESS = settings.BROKER_ADDRESS
MQTT_PORT = settings.MQTT_PORT


# Connection state variable
Connected = False
TIMEOUT = settings.TIMEOUT

device_dictionary = {}
device_list = []


def create_dictionary(warehouse_id, device_id):
    """Creates a dictionary for every device.
    Keeps track of message count.
    Keeps track of time out

    Args:
        warehouse_id (str): Warehouse ID of the device
        device_id (str): Device ID of the device

    Returns:
        dict: Dictionary containing device information
    """

    logging.info(f"Creating dictionary for %s/%s" % (warehouse_id, device_id))
    return {
        "warehouse_id": warehouse_id,
        "device_id": device_id,
        "message_count": 0,
        "message_limit": settings.MESSAGE_LIMIT,
        "message_arr": [],
        "end_time": -1,
        "pub_topic": f"/{warehouse_id}/{device_id}",
    }


def reset_variables(device_name):
    """Resets variables for a specific device

    Args:
        device_name (str): Combination of warehouseID and deviceID
    """

    global device_dictionary

    lock.acquire()

    try:
        device_dictionary[device_name]["message_count"] = 0
        device_dictionary[device_name]["message_arr"] = []
        device_dictionary[device_name]["end_time"] = -1
        logging.info('Reset value for %s' % device_name)
    except Exception as e:
        logging.error('Reset for %s failed - %s' % (device_name, e))
    finally:
        # Always called even if exception is raised
        lock.release()


"""
CALLBACKS
"""


def on_connect(client, userdata, flags, rc):

    if rc == 0:
        logging.info("Connected to broker")
        global Connected
        Connected = True

    else:
        logging.error("Connection failed from ec2 %s",str(rc))


def on_disconnect(client, userdata, rc):
    if rc == 0:
        global Connected
        Connected = False
        logging.info("Disconnected")
    else:
        logging.error("Still running in the ec2")


def on_message(client, userdata, message):
    global device_list, device_dictionary

    # Read the message from the device
    msg = str(message.payload.decode("utf-8"))

    # Extract device id and warehouse id
    warehouse_id = msg.split(",")[-2].strip()
    device_id = msg.split(",")[-1].strip()

    # Create device name
    device_name = f"{warehouse_id}/{device_id}"

    # Create device dictionary
    if device_name not in device_list:
        device_dictionary[device_name] = create_dictionary(
            warehouse_id, device_id
        )
        device_list.append(device_name)

    # Add message to device dictionary's message array
    device_dictionary[device_name]["message_arr"].append(msg)

    # Increment message count by 1
    device_dictionary[device_name]["message_count"] += 1

    # If device dictionary's message count is equal to the messsage limit
    if (device_dictionary[device_name]["message_count"]
            == device_dictionary[device_name]["message_limit"]
       ):

        # Assign the device dictionary parameters to variables
        message_arr = device_dictionary[device_name]["message_arr"]
        pub_topic = device_dictionary[device_name]["pub_topic"]

        # Get device settings from PSQL Table
        try:
            fruit, variety, white_standard, batch_number, vendor_code = psql_func.get_device_data(warehouse_id, device_id)[0]
            white_standard = [float(x) for x in white_standard.values()]

            brix_model = BRIX_MODEL_DICT[fruit][variety]
            clf_model = CLF_MODEL_DICT[fruit][variety]

        except Exception as e:
            logging.critical("Failed to load device data - %s" % e)

            fruit, variety = 'default', 'default'
            batch_number, vendor_code = 'default', 'default'
            white_standard = settings.DEFAULT_WHITE_STANDARD

            brix_model = BRIX_MODEL_DICT['default']
            clf_model = CLF_MODEL_DICT['default']

        # Normalize the message array with respective white standard
        raw_mean_values, normalized_values = calculations.normalize_fruit_data(
            message_arr, white_standard
        )

        # Predict brix
        try:
            predicted_brix = calculations.predict_brix(normalized_values, brix_model)
        except Exception as e:
            logging.error("Brix prediction failed - %s")
            predicted_brix = -1

        # Assign a category to the brix value
        brix_level = calculations.calculate_brix_level(predicted_brix)

        # Classify status
        try:
            fruit_status = calculations.predict_status(normalized_values, clf_model)
        except Exception as e:
            logging.error("Status classification failed - %s")
            fruit_status = -1

        # Send feedback
        message_to_client = f"{str(fruit_status)}{brix_level},{round(float(predicted_brix), 2)};"
        print(message_to_client)
        client.publish(pub_topic, message_to_client)

        # Update to DB
        try:
            psql_func.write_data(warehouse_id,
                                 device_id,
                                 raw_mean_values,
                                 predicted_brix,
                                 fruit_status,
                                 fruit,
                                 variety,
                                 batch_number,
                                 vendor_code)

        except Exception as e:
            logging.error("Failed to store data in PSQL: %s" % e)


        # Resets variables for device
        reset_variables(device_name)


"""
CLIENT FUNCTIONS
"""


def create_client():
    # Create client instance
    client = mqttClient.Client()
    client.username_pw_set(USER, password=PASSWORD)

    # Callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    return client


def client_disconnect(client):
    logging.info(f"Disconnecting via Script ({USER})")
    client.disconnect()


"""
MAIN LOOP
"""

if __name__ == "__main__":

    # Threading lock (to prevent to write statements occuring at the same time)
    lock = threading.Lock()

    # Create client
    client = create_client()

    # Connect
    client.connect(BROKER_ADDRESS, port=MQTT_PORT)
    logging.info(f"Connected via Script ({USER})")

    # Subscribe to MQTT Topic
    client.subscribe(SUB_TOPIC)

    # Load pickle models for brix
    try:

        BRIX_MODEL_DICT = {'default': settings.DEFAULT_BRIX_MODEL}
        CLF_MODEL_DICT = {'default': settings.DEFAULT_CLF_MODEL}

        # List of tuples -> [(fruit, variety), (fruit, variety)]
        FRUIT_VARIETY_LIST = psql_func.get_fruit_variety_list()

        DEFAULT_BRIX_MODEL = settings.DEFAULT_BRIX_MODEL
        DEFAULT_CLF_MODEL = settings.DEFAULT_CLF_MODEL

        for fruit, variety in FRUIT_VARIETY_LIST:

            # Create dictionary for fruit if it doesn't exist
            if fruit not in BRIX_MODEL_DICT:
                BRIX_MODEL_DICT[fruit] = {}

            BRIX_MODEL_DICT[fruit][variety] = None

            # Create dictionary for fruit if it doesn't exist
            if fruit not in CLF_MODEL_DICT:
                CLF_MODEL_DICT[fruit] = {}

            CLF_MODEL_DICT[fruit][variety] = None

            # Assign models to variables
            brix_file = f"{settings.MODEL_DIR}BRIX_{fruit}_{variety}.sav"
            clf_file = f"{settings.MODEL_DIR}CLF_{fruit}_{variety}.sav"

            # Load models and store in respective dictionary
            try:
                BRIX_MODEL_DICT[fruit][variety] = pickle.load(open(brix_file, 'rb'))
            # Else, load default model and store it in respective dictionary
            except:
                BRIX_MODEL_DICT[fruit][variety] = pickle.load(open(DEFAULT_BRIX_MODEL, 'rb'))
                logging.info("Using default brix model for %s-%s" %(fruit, variety))

            # Load models and store in respective dictionary
            try:
                CLF_MODEL_DICT[fruit][variety] = pickle.load(open(clf_file, 'rb'))
            # Else, load default model and store it in respective dictionary
            except:
                CLF_MODEL_DICT[fruit][variety] = pickle.load(open(DEFAULT_CLF_MODEL, 'rb'))
                logging.info("Using default clf model for %s-%s" %(fruit, variety))

        logging.info("Models loaded")

    except Exception as e:
        logging.error("Failed to load models - %s" % e)


    # Start listening
    client.loop_start()

    # Time out functionality
    while True:

        for device_name in device_list:

            end_time = device_dictionary[device_name]["end_time"]
            message_count = device_dictionary[device_name]["message_count"]
            message_limit = device_dictionary[device_name]["message_limit"]

            # If device has sent one or more messages
            if message_count >= 1:

                # If timeout has not been set (when message_count == 1)
                if end_time == -1:
                    # Sets end_time to current time + TIMEOUT
                    device_dictionary[device_name]["end_time"] = time.time() + TIMEOUT
                    logging.info(f"Timeout initiated for {device_name}")

                # If timeout has been set
                if end_time != -1:
                    # If time is past the end_time, resets the variables
                    if time.time() > end_time:
                        logging.error(f"Timeout exceeded for {device_name}")
                        reset_variables(device_name)

    client.loop_stop()
    client.disconnect()
