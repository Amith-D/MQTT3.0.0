# Scientific Libraries
import numpy as np

# Custom modules
import settings


def normalize_fruit_data(data, white_standard):
    """Converts the device readings to float values.
    The average values of the set of readings are normalized and returned

    Args:
        data (list): List of strings containing device readings
        white_standard (list): Normalization values for specific device

    Returns:
        list: Both raw values and normalized values
    """
    LENGTH = len(white_standard)

    """Converting sensor readings to float
    Converts all readings except Warehouse ID and Device ID
    '1, 2, 3, 4, 5, 6, 82.5, WC0001, D20' -> [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 82.5]
    """
    sensor_data = np.array([list(map(float, i.split(', ')
                                     [: -2])) for i in data])

    # Calculate mean values
    raw_mean_values = np.mean(sensor_data, axis=0)

    # Normalizes only the wavelength values
    normalized_values = (raw_mean_values[: LENGTH] / white_standard)

    return raw_mean_values, normalized_values


def predict_status(values, model):
    """Classifies fruit status for given wavelength values into good and bad.
    0 -> Bad
    1 -> Good

    Args:
        values (list): Sensor readings (wavelength)
        model (linear model): Model to classify fruits

    Returns:
        int: The classification value of the fruit
    """
    try:
        values = np.array([values])
        predictions = model.predict_proba(values[0:1])
        fruit_status = predictions[0][1]
    except Exception as e:
        print("ERROR",e)
        fruit_status = -1
    print(fruit_status)
    
    r,g,b = ((255 - int(fruit_status*255)),int(fruit_status*255),0)
    #fruit_status = fruit_status+'% GOOD'
    return int(fruit_status*100),r,g,b


def predict_brix(values, model):
    """Predicts the brix value for given wavelength values

    Args:
        values (list): Sensor readings (wavelength)
        model (regression model): Model to predict brix values

    Returns:
        float: The predicted brix value
    """

    values = np.array([values])
    predicted_brix_value = model.predict(values)

    # Work around cause some models return nested values
    try:
        pred = predicted_brix_value[0][0]
    except Exception:
        pred = predicted_brix_value[0]

    return pred


def calculate_brix_level(predicted_brix):
    """Finds the range which the brix value belongs to

    Args:
        predicted_brix (float): The predicted brix value

    Returns:
        str: The range under which the brix value falls
    """
    if predicted_brix < 9:
        return 'A'
    elif predicted_brix >= 9 and predicted_brix < 12:
        return 'B'
    elif predicted_brix >= 12 and predicted_brix < 15:
        return 'C'
    elif predicted_brix >= 15 and predicted_brix < 18:
        return 'D'
    return 'E'
