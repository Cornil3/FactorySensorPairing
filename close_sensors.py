"""
get all the sensors within 1 KM of a factory
"""

import pandas as pd
import requests
from pandas import DataFrame
from typing import Tuple
from shapely import wkt


BUFFER_SIZE = 1.5 # how close a sensor has to be to a factory (KM)


def main():
    factory_data, sensors_data = get_data()

    sensors_data = prepare_sensors_data(sensors_data)
    factory_data = prepare_factory_data(factory_data)

    all_intersected_sensors_df = get_intersected_sensors(sensors_data, factory_data)
    all_intersected_sensors_df.to_csv("sensors_close_to_factories.csv", encoding='utf-8', index=False)


def get_data() -> Tuple[DataFrame, DataFrame]:
    """
    query sensors data from sviva API and load factories data from csv file
    """
    resp = requests.get("https://www.svivaaqm.net/ajax/getAllStationsNew?_=%3CEPOCH_TIMESTAMP%3E")
    if resp.status_code != 200:
        print("Got Bad Response: ", resp.status_code, resp.text)
    sensors_raw_data = resp.json()['Stations']
    sensors_data = pd.DataFrame(sensors_raw_data)
    factory_data = pd.read_csv("factory_data.csv")
    return factory_data, sensors_data


def prepare_sensors_data(sensors_data: DataFrame) -> DataFrame:
    """
    keep only sensors that have latitude and longitude data
    create a new 'point' column with wkt representation of the sensors location
    create a new 'geo' column with shapely objects of the sensors locations
    """
    sensors_data = sensors_data[sensors_data['longitude'].apply(lambda x: len(x) > 0)]
    sensors_data = sensors_data[sensors_data['latitude'].apply(lambda x: len(x) > 0)]
    sensors_data = sensors_data.drop_duplicates(subset=['latitude', 'longitude'])
    sensors_data['point'] = sensors_data.apply(lambda row: f"POINT ({row['longitude']} {row['latitude']})", axis=1)
    sensors_data['geo'] = sensors_data['point'].apply(wkt.loads)
    return sensors_data


def prepare_factory_data(factory_data: DataFrame) -> DataFrame:
    """
    make every row in the table unique to a factory location
    create a new 'point' column with wkt representation of the factory location
    create a new 'geo' column with shapely objects of the factory locations
    create a new 'geo_buffer' column with a buffer of a factory location
    """
    # factory x and y are inverted in the csv file (my bad)
    factory_data = factory_data.drop_duplicates(subset=['x', 'y'])
    factory_data['point'] = factory_data.apply(lambda row: f"POINT ({row['y']} {row['x']})", axis=1)
    factory_data['geo'] = factory_data['point'].apply(wkt.loads)
    factory_data['geo_buffer'] = factory_data['geo'].apply(lambda r: r.buffer(BUFFER_SIZE / 111))
    return factory_data


def get_intersected_sensors(sensors_data: DataFrame, factory_data: DataFrame) -> DataFrame:
    """
    get all the sensors that are within each factory's buffer
    """
    all_intersected_sensors = []
    for i, factory in factory_data.iterrows():
        buffer = factory['geo_buffer']
        intersected_sensors = sensors_data[sensors_data['geo'].apply(lambda g: g.intersects(buffer))]
        intersected_sensors = intersected_sensors[['serialCode', 'height', 'latitude', 'longitude']]
        intersected_sensors['factory_name'] = factory['name']
        intersected_sensors['factory_latitude'] = factory['x']
        intersected_sensors['factory_longitude'] = factory['y']
        all_intersected_sensors.append(intersected_sensors.drop_duplicates(subset=['serialCode']))
    all_intersected_sensors_df = pd.concat(all_intersected_sensors)
    return all_intersected_sensors_df


if __name__ == '__main__':
    main()
