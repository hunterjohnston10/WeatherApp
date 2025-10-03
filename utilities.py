import unified
import pandas as pd
from datetime import datetime, timezone

def to_timestamp(datetime_object):
    return f"{datetime_object.year}-{datetime_object.month}-{datetime_object.day}"

def convert_weather_data(hourly_data, preferred_units):
    for index, value in preferred_units.items():
        hourly_data[index] = hourly_data[index].pint.to(value)
    return hourly_data

def get_all_weather_data(location: str, start_date: str, end_date: str, ureg):
    hourly_variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "pressure_msl",
        "wind_speed_10m",
        "wind_direction_10m",
        "pm2_5",
        "pm10",
        "nitrogen_dioxide",
        "carbon_monoxide",
        "ozone"
    ]
    daily_variables = ['uv_index_max']

    hourly_data = pd.DataFrame()
    daily_data = pd.DataFrame()

    for variable in hourly_variables:
        data = unified.fetch_unified(variable, 
                                     location,
                                     'both',
                                     start_date,
                                     end_date)
        
        unit = ureg(data['units'][variable])

        print(unit)

        parsed_data = pd.DataFrame.from_dict(data['data'])
        parsed_data['timestamp_utc'] = pd.to_datetime(parsed_data['timestamp_utc']).dt.tz_localize('UTC')
        parsed_data[variable] = parsed_data[variable].astype(f"pint[{unit}]")

        if hourly_data.empty:
            hourly_data = parsed_data
        else:
            hourly_data = hourly_data.merge(parsed_data, how='outer', on='timestamp_utc')
    
    for variable in daily_variables:
        data = unified.fetch_unified(variable, 
                                     location,
                                     'both',
                                     start_date,
                                     end_date)
        
        unit = ureg(data['units'][variable])

        parsed_data = pd.DataFrame.from_dict(data['data'])
        parsed_data['date'] = pd.to_datetime(parsed_data['date']).dt.tz_localize('UTC')
        parsed_data[variable] = parsed_data[variable].astype(f"pint[{unit}]")

        if daily_data.empty:
            daily_data = parsed_data
        else:
            daily_data = daily_data.merge(parsed_data, how='outer', on='date')
    
    return hourly_data

