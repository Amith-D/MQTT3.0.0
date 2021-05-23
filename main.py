# Basic libraries
import logging
import threading
import time
import pickle
from warnings import filterwarnings
from time import sleep
import numpy as np

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


def create_dictionary(mac_id):
    """Creates a dictionary for every device.
    Keeps track of message count.
    Keeps track of time out

    Args:
        warehouse_id (str): Warehouse ID of the device
        device_id (str): Device ID of the device

    Returns:
        dict: Dictionary containing device information
    """

    
    return {
        "mac_id": mac_id,
        "message_count": 0,
        "message_limit": settings.MESSAGE_LIMIT,
        "message_arr": [],
        "end_time": -1,
        "pub_topic": f"/{mac_id}",
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
        logging.info("Connected to broker successfully")
        global Connected
        Connected = True

    else:
        logging.error("Connection failed printing rc " )
        print(rc)


def on_disconnect(client, userdata, rc):
    if rc == 0:
        global Connected
        Connected = False
        logging.info("Disconnected")
    else:
        logging.error("Still running printing rc ")


def on_message(client, userdata, message):
    global device_list, device_dictionary

    # Read the message from the device into msg
    msg = str(message.payload.decode("utf-8"))
    #Strip mac_id from the last 
    mac_id = msg.split(",")[-1].strip()


    #Boot call from a device
    #Provide the model its running on+
    #Provide the formatted(shortened) list of all fruits and varieties
    if (msg.split(",")[0].strip()=='MR'):
        try:
            #Get the model its currently running on
            dev_model_name = psql_func.read_most_recent_fruit_id(msg.split(",")[-1].strip())
            dev_model_name = dev_model_name[0]+'['+dev_model_name[1][0:2]+']'
            #print("Model_recieved",dev_model_name)
            #Get the list of models
            FRUIT_VARIETY_LIST = psql_func.get_fruit_variety_list()
            f_v_list = ''
            #Get the shortened list
            for i in range(len(FRUIT_VARIETY_LIST)):
                f_v_list = f_v_list + str(','+FRUIT_VARIETY_LIST[i][0]+'['+(FRUIT_VARIETY_LIST[i][1])[0:2]+']')
            #print(FRUIT_VARIETY_LIST)
            message_to_client = f"!{dev_model_name}{f_v_list}"
            pub_topic = f"/{mac_id}"
            #print('pub_topic = ',pub_topic)
            #print("This is the message length ",len(message_to_client))
            client.publish(pub_topic,message_to_client)
            #print("Device_informed")
        except Exception as e:
            #print(e)
            logging.error("BOOT FAIL !!!")
        reset_variables(device_name)


    #Model change request
    #Takes model shortened name as input
    elif(msg.split(",")[0].strip()=='MC'):
        #print("MC started")
        #print(msg.split(",")[-2].strip())
        try:
            variety_list_dict = {'APPLE[GR]':2,'APPLE[SH]':3,'APPLE[FU]':4,'APPLE[GA]':5,'APPLE[RE]':6,'BANANA[RO]':9,'MANGO[AL]':1,'CITRUS[OR]':10,'CITRUS[MO]':11,'WHITE STD[WH]':12,'APPLE[KI]':13,'CITRUS[KI]':14,'GRAPE[GS]':15,'GRAPE[BS]':16,'GRAPE[RE]':17,'GRAPE[TH]':18}
            fruit_vid = variety_list_dict[msg.split(",")[-2].strip()]
            new_model_no = psql_func.change_fruit_variety(fruit_vid,msg.split(",")[-1].strip())
            message_to_client = f"@{new_model_no}"
            pub_topic = f"/{mac_id}"
            #print(pub_topic)
            #print("This is the message ",message_to_client
            client.publish(pub_topic, message_to_client)
            #print('done')
        except Exception as e:
            #print(e)
            logging.error("Failed to change the model")
            reset_variables(device_name)



    # Extract device id and warehouse id
    #warehouse_id = msg.split(",")[-2].strip()
    #mac_id = msg.split(",")[-1].strip()
    # Create device name
    device_name = f"/{mac_id}"

    # Create device dictionary
    if device_name not in device_list:
        device_dictionary[device_name] = create_dictionary(mac_id)
        device_list.append(device_name)

    # Add message to device dictionary's message array
    device_dictionary[device_name]["message_arr"].append(msg)

    # Increment message count by 1
    device_dictionary[device_name]["message_count"] += 1

    # If device dictionary's message count is equal to the messsage limit

    if (device_dictionary[device_name]["message_count"]
            == device_dictionary[device_name]["message_limit"]
       ):
        #print('entered if')
        # Assign the device dictionary parameters to variables
        #print(device_dictionary)
        message_arr = device_dictionary[device_name]["message_arr"]
        pub_topic = device_dictionary[device_name]["pub_topic"]
    
        # Get device settings from PSQL Table
        try:
            #print('Entered try')
            fruit, variety, white_standard, batch_number, vendor_code, device_id, warehouse_id = psql_func.get_device_data(mac_id)[0]
            #print('get_device_data',psql_func.get_device_data(mac_id)[0])
            #print('warehouse_id',warehouse_id)
            white_standard = [float(x) for x in white_standard.values()]
            #print(BRIX_MODEL_DICT[fruit][variety])
            brix_model = BRIX_MODEL_DICT[fruit][variety]
            clf_model = CLF_MODEL_DICT[fruit][variety]

        except Exception as e:
            print('Failed to load device data',e)
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
            logging.error("Brix prediction failed - %s" % e)
            predicted_brix = -1

        # Assign a category to the brix value
        #brix_level = calculations.calculate_brix_level(predicted_brix)

        # Classify status
        try:
            normalized_values = np.append(normalized_values,predicted_brix ,axis=None)
            fruit_status,r,g,b = calculations.predict_status(normalized_values, clf_model)
            p = str(fruit_status)+'% GOOD'
        except Exception as e:
            print("error calculating status",e)
            logging.error("Status classification failed - %s" % e)
            fruit_status = -1

        # Send feedback
        print("about to send feedback")
        #r,g,b = 255,255,255
        print(f"${round(float(predicted_brix), 2)},{str(fruit_status)},{r},{g},{b};")
        message_to_client = f"${round(float(predicted_brix), 2)},{p},{r},{g},{b};"
        print("message_to_client = ",message_to_client)
        client.publish(pub_topic, message_to_client)
        #print("printing pridicted brix",predicted_brix)

        # Update to DB
        try:
            print('Updating to DB')
            psql_func.write_data(warehouse_id,
                                 device_id,
                                 raw_mean_values,
                                 predicted_brix,
                                 fruit_status,
                                 fruit,
                                 variety,
                                 batch_number,
                                 vendor_code,
                                 mac_id)
            print("Successfully uploaded psql data")

        except Exception as e:
            print(e)
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
        print(FRUIT_VARIETY_LIST)

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
                print("Failed to load model",fruit,variety)
                logging.info("Using default brix model for %s-%s" %(fruit, variety))

            # Load models and store in respective dictionary
            try:
                CLF_MODEL_DICT[fruit][variety] = pickle.load(open(clf_file, 'rb'))
            # Else, load default model and store it in respective dictionary
            except:
                print("Failed to load clfmodel",fruit,variety)
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
