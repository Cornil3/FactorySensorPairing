import math
from typing import Tuple, List

import pandas as pd
import pyodbc

DBLOGIN = {
    "user": "house-keeper",
    "pass": "Password1",
    "host": "house-keepers.database.windows.net",
    "db": "Keepers_hb"
}

MAX_WIND_OPENING = 90
WIND_PERCENTILE = 80
QUERYTIME = 1  # time back to query (days)
TABLE = "alerts"


# TODO: finish get_sensor_deviations
# TODO: implement check_materials
# TODO: implement write_to_db function

def main():
    close_sensors = pd.read_csv("sensors_close_to_factories.csv")
    factories = pd.read_csv("factory_data.csv")
    for sensor_id, sensor_data in close_sensors.groupby(by="serialCode"):
        factory_data = factories[factories['name'] == sensor_data['factory_name'].iloc[0]]
        all_sensor_deviations = get_sensor_deviations(sensor_id)
        for deviation_data in all_sensor_deviations:
            wind_area = check_wind(deviation_data)
            if factory_in_wind_opening(sensor_data, factory_data, wind_area) and check_materials(deviation_data,
                                                                                                 factory_data):
                deviation_data['predicted_factory'] = factory_data['factoryId']
                deviation_data['sensor'] = sensor_id
                write_to_db(deviation_data)


def get_sensor_deviations(sensor_id: str) -> List[pd.DataFrame]:
    """
    query the DB for all the sensor's deviations between QUERYTIME and now
    """
    query = f"""SELECT * FROM keepers_hb.dbo.{TABLE} WHERE StationId = {sensor_id} AND Timestamp > DATEADD(day,-{QUERYTIME}, GETDATE())"""
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};' +
                        'SERVER=' + DBLOGIN['host'] +
                        ';DATABASE=' + DBLOGIN['db'] +
                        ';UID=' + DBLOGIN['user'] +
                        ';PWD=' + DBLOGIN['pass']) as conn:
        result = pd.read_sql(query, conn)
        grouped_alerts = result.groupby(by="AlertId")
        alerts_dfs = [group for _, group in grouped_alerts]
        return alerts_dfs


def check_wind(pollution_data: pd.DataFrame) -> Tuple[float, float]:
    """
    calculate the min and max angles of the wind over the 80th percentile of the pollution time
    return None if the is more than 90 degrees between the min and max values of the winds
    :param pollution_data: data from sensor during a pollution event
    :return: tuple with min and max angles
    """
    wind_dir = pollution_data['wind_dir']
    average_wind_dir = wind_dir.mean()
    min_angle = average_wind_dir - 15 % 360
    max_angle = average_wind_dir + 15 % 360
    percent_samples = calc_percent_of_samples(min_angle, max_angle, wind_dir)
    for i in range(101):
        min_angle_der = (calc_percent_of_samples(min_angle + 5 % 360, max_angle, wind_dir) - percent_samples) / 5
        max_angle_der = (calc_percent_of_samples(min_angle, max_angle + 5 % 360, wind_dir) - percent_samples) / 5
        min_angle_step = (WIND_PERCENTILE - percent_samples) / (min_angle_der * 10)
        max_angle_step = (WIND_PERCENTILE - percent_samples) / (max_angle_der * 10)
        min_angle = (min_angle + min_angle_step) % 360
        max_angle = (max_angle + max_angle_step) % 360
        percent_samples = calc_percent_of_samples(min_angle, max_angle, wind_dir)
        if WIND_PERCENTILE - 5 < percent_samples < WIND_PERCENTILE + 5:
            break
    return min_angle, max_angle


def calc_percent_of_samples(min_angle: float, max_angle: float, wind_dir: pd.Series) -> float:
    """
    calculate the percent of wind samples that are between min and max angles
    """
    # for cases when min angle went under 0 or max angle went over 360
    if min_angle > max_angle:
        samples_in_range = wind_dir[(wind_dir > min_angle) | (wind_dir < max_angle)]
    else:
        samples_in_range = wind_dir[(wind_dir > min_angle) & (wind_dir < max_angle)]
    return len(samples_in_range) / len(wind_dir)


def check_materials(pollution_data: pd.DataFrame, factory: pd.Series) -> bool:
    """
    return True if at least one of the pollution sources is emmited by the factory
    :param pollution_data: data from sensor during a pollution event
    :param factory: the factory we check for sources
    :return:
    """


def write_to_db(pollution_data: pd.DataFrame):
    pass


def factory_in_wind_opening(sensor: pd.Series, factory: pd.Series, wind_area: Tuple[float, float]) -> bool:
    """
    check if the angle between the factory and the sensor is between the min and max wind angles
    :param sensor: the sensor we check
    :param factory: the factory we check
    :param wind_area: (min_angle, max_angle) of the wind
    :return: if the factory is within the wind opening
    """
    delta_x = sensor['longitude'] - factory['y']
    delta_y = sensor['latitude'] - factory['x']
    angle = math.atan(delta_y / delta_x) * 180 / math.pi
    return wind_area[0] < angle < wind_area[1]


if __name__ == '__main__':
    main()
