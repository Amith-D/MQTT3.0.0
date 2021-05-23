"""
Settings for the scripts
"""

# Global settings
BASE_DIR = "/home/ubuntu/qzense_mqtt/"
LOG_DIR = f"{BASE_DIR}log/"

MQTT_LOG_FILE = f'{LOG_DIR}mqtt.log'
STATUS_UPDATE_LOG_FILE = f'{LOG_DIR}status_update.log'

MODEL_DIR = f'{BASE_DIR}models/'
TIMEZONE = 'Asia/Kolkata'


# MQTT Settings
BROKER_ADDRESS = '65.1.238.16'
MQTT_PORT = 1883

SUB_TOPIC = '/proto/out'
UPDATE_SUB_TOPIC = '/update/out'

MQTT_USER = 'Qzense'
MQTT_PASSWORD = 'Qzenselabs'

MQTT_USER2 = "proto"
MQTT_PASSWORD2 = "qzense"


# Device Settings
TIMEOUT = 10
MESSAGE_LIMIT = 2

DEFAULT_BRIX_MODEL = f"{MODEL_DIR}default_brix.sav"
DEFAULT_CLF_MODEL = f"{MODEL_DIR}default_clf.sav"

DEFAULT_WHITE_STANDARD = [1, 1, 1, 1, 1, 1]
DEVICE_READINGS = ['610nm', '680nm', '730nm', '760nm', '810nm', '860nm', 'temp', 'humidity', 'temperature']

# SSH Credentials
REMOTE_HOST = "65.1.238.16"
REMOTE_SSH_PORT = 22

REMOTE_USERNAME = "ubuntu"
REMOTE_PRIVATE_KEY = f"{BASE_DIR}keys/django_ec2.pem"


# PSQL Settings
PSQL_PORT = 5432
PSQL_DB_NAME = 'ebdb'

PSQL_HOST = 'aatdzkkric86km.cbudtrkx2byn.ap-south-1.rds.amazonaws.com'
PSQL_USER = 'qZenseTEST'
PSQL_PASSWORD = 'eFirst2019'

PSQL_MAIN_TABLE = 'warehouse_data'
PSQL_DEVICE_SETTINGS_TABLE = 'devices'
