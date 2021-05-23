# SSH and PSQL Library
from sshtunnel import SSHTunnelForwarder
import psycopg2

# Misc Libraries
import json
from datetime import datetime
import pytz

# Custom Modules
import settings
import math

# Global settings
DEVICE_SETTINGS_TABLE = settings.PSQL_DEVICE_SETTINGS_TABLE
DEVICE_READINGS = settings.DEVICE_READINGS
TIMEZONE = pytz.timezone(settings.TIMEZONE)


def create_dictionary(keys, values):
    """Creates a dictionary of sensor names and its respective sensor values
    {
        '<sensor_key>': <sensor_value>,
        '810nm': '42.52'
    }
    Sensor value in the dictionary will be a string.

    Args:
        keys (list): list of sensor names
        values (list): list of sensor values

    Returns:
        json: dictionary of sensor name and respective sensor values
    """
    return json.dumps({key: value for key, value in zip(keys, values)})


def write_data(warehouse_id,device_id, device_readings, brix, status, fruit, variety, batch_number, vendor_code,mac_id):
    """ Writes the data passed to the main table

    Parameters
    ----------
    warehouse_id: str
        Warehouse ID of the device
    device_id: str
        Device ID of the device
    device_readings: list of float values
        List of data collected by the device
    brix: float
        The brix predicted by the model for the current reading
    status: float
        The status of the fruit for the current reading
    """


    # Time related data
    now = datetime.now(tz=TIMEZONE)
    date_stamp = str(now.date())
    time_stamp = str(now.time())

    # Unique Key
    device_info = f"{warehouse_id}/{device_id}/{date_stamp}/{time_stamp}"
    print(device_info)
    print(brix)
    # ID (Primary Key)
    id_pk = read_most_recent_id()[0] + 1
    print(device_readings)
    for reading in range(len(device_readings)):
        if math.isnan(device_readings[reading]):
            device_readings[reading]=0.0
    print(device_readings)        
    
    # Device readings
    sensor_dict = create_dictionary(DEVICE_READINGS, device_readings)

    # SQL insert query
    insert_query = """INSERT INTO warehouse_data(id,warehouse_id,device_id,
                                                 fruit,variety,batch_number,
                                                 brix,status,date,timestamp,device_info,
                                                 wv_610nm,wv_680nm,wv_730nm,wv_760nm,wv_810nm,wv_860nm,mac_id)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

    # Data to be inserted
    params = (id_pk, warehouse_id, device_id, fruit, variety,
              batch_number, brix, status, date_stamp, time_stamp, device_info,
              device_readings[0],device_readings[1],device_readings[2],device_readings[3],
              device_readings[4],device_readings[5],mac_id)

    # Executing and commiting
    print(params)
    x  = cur.execute(insert_query, params)
    #print(cur.fetch())
    conn.commit()
    print("Done with write")


def get_device_data(mac_id: str):
    """ Returns a device's settings

    Parameters
    ----------
    warehouse_id: str
        Warehouse ID of the device
    device_id: str
        Device ID of the device

    Returns
    -------
    The fruit name, fruit variety, batch number and vendor code
    associated with the device
    """
    #print('psql')
    try:
        fetch_query = """SELECT fruit_name AS fruit, variety, white_standard, batch_number, vendor_code,device_id,W.warehouse_id FROM devices D, fruit_varieties V, fruits F, device_types T,warehouses W WHERE D.mac_id=%s AND D.FRUIT_VARIETY_ID = V.ID AND V.FRUIT_ID = F.ID AND D.device_type_id = T.id AND D.warehouse_id = W.id"""
    except Exception as e:
        print(e)
    #print(fetch_query)
    cur.execute(fetch_query, (mac_id,))
    response = cur.fetchall()
    #print ( "response = ", response)
    return response


def read_most_recent_id():
    """
    Returns the most recent ID from the warehouse data table
    """
    query = """SELECT ID FROM warehouse_data
                ORDER BY id DESC LIMIT 1;"""

    try:
        cur.execute(query)
        return cur.fetchall()[0]
    except Exception:
        return -1


def read_most_recent_item(warehouse_id, device_id):
    """
    params: warehouse_id, device_id: To uniquely identify the device
    return: Latest status value and device info for specific device
    """
    query = """SELECT status, device_info from warehouse_data WHERE warehouse_id=%s AND device_id=%s
                ORDER BY id DESC limit 1"""

    try:
        cur.execute(query, (warehouse_id, device_id))
        return cur.fetchall()[0]
    except:
        return (-1, -1)


def update_item(status, device_info):
    """
    params: fruit_status: The flipped fruit status to be updated
            device_info: To uniquely identify the device
    return: The updated status
    """
    status = 1 if status == 0 else status

    if device_info != -1:
        query = """UPDATE warehouse_data SET status=%s WHERE device_info=%s"""
        cur.execute(query, (str(status), device_info))
        conn.commit()

    return status


def flip_status(warehouse_id, device_id):
    """
    params: warehouse_id, device_id: To uniquely identify the device
    return: The updated status value
    """

    status, device_info = read_most_recent_item(warehouse_id, device_id)
    status = update_item(status, device_info)
    return status


def get_fruit_variety_list():
    fetch_query = """SELECT fruit_name AS fruit, variety from fruits, fruit_varieties where fruit_varieties.fruit_id = fruits.id"""
    cur.execute(fetch_query)
    response = cur.fetchall()
    return response



def read_most_recent_fruit_id(mac_id):
    """Returns the most recent ID from the warehouse data table"""
    query = """SELECT  F.fruit_name , V.variety 
               FROM fruit_varieties AS V
               JOIN devices AS D  
               ON D.fruit_variety_id  = V.id 
               join fruits as F
               on V.fruit_id = F.id
               where d.mac_id =%s"""
    print("read_most_recent",mac_id)
    try:
        print(type(mac_id),mac_id)
        cur.execute(query,(mac_id,))
        return cur.fetchall()[0]
    except Exception as e:
        print(e)
        return -1

def change_fruit_variety(model_id,mac_id):
    print('changing fruit variety') 
    fetch_query = """UPDATE public.devices SET fruit_variety_id =%s::integer WHERE mac_id =%s;"""
    try:
        cur.execute(fetch_query,(model_id,(mac_id,)))
        conn.commit()
        print('donee')
        response = model_id
    except Exception as e:
        print('oops')
        print(e)
        respose = cur.fetchhall()[0]
    print(response)
    return response
conn = psycopg2.connect(database=settings.PSQL_DB_NAME,
                        user=settings.PSQL_USER,
                        password=settings.PSQL_PASSWORD,
                        host=settings.PSQL_HOST,
                        port=settings.PSQL_PORT)

cur = conn.cursor()

if __name__ == '__main__':

    warehouse_id, device_id = 'BLR_1', 'DEV_1'

    r = get_device_data(warehouse_id, device_id)
    print(r[0])
    f, v, w, b, vc, t = r[0]
    print(f, v, b, vc, t)

    device_readings = [5733.02, 1181.52, 284.845, 184.935, 229.075, 158.735, 81.2] 
    brix = 120
    status = 1

    write_data(warehouse_id, device_id, device_readings, brix, status, f, v, b, vc)
