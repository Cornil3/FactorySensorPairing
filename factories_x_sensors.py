import requests
import pandas as pd
import numpy as np
from scipy import spatial
from typing import List
import plotly.express as px
import math

# TODO: How many factories are the closest to a sensor bar chart
# TODO: How many closest factories pollute the same chemicals the sensor can detect


def main():
    resp = requests.get("https://www.svivaaqm.net/ajax/getAllStationsNew?_=%3CEPOCH_TIMESTAMP%3E")
    if resp.status_code != 200:
        print("Got Bad Response: ", resp.status_code, resp.text)
    sensors_data = resp.json()['Stations']
    factory_data = pd.read_csv("factory_data.csv")

    sensor_x = [float(sensor['longitude']) for sensor in sensors_data if len(sensor['longitude']) > 0]
    sensor_y = [float(sensor['latitude']) for sensor in sensors_data if len(sensor['latitude']) > 0]
    sensors_coords = get_coords_array(sensor_x, sensor_y)

    # factory x and y are inverted in the csv file (my bad)
    factory_y = factory_data['x'].drop_duplicates()
    factory_x = factory_data['y'].drop_duplicates()
    factory_coords = get_coords_array(factory_x, factory_y)
    result = spatial.distance.cdist(factory_coords, sensors_coords, 'euclidean')

    factory_sensor_min_dist = list(map(min, result*111)) # multiply by 111 to get kilometers
    factory_sensor_min_dist = list(map(math.ceil, factory_sensor_min_dist))
    factory_sensor_min_dist.sort()

    df = pd.DataFrame(factory_sensor_min_dist)
    df = df.value_counts().rename_axis('Sensor Distance From Factory (KM)').reset_index(name='Factories')
    fig = px.bar(df, x='Sensor Distance From Factory (KM)', y='Factories')
    fig.show()


def get_coords_array(x: List[float], y: List[float]) -> np.array:
    return np.array(list(zip(x, y)))


if __name__ == "__main__":
    main()