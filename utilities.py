import unified
import pandas as pd
import pint
import streamlit as st

ureg = pint.UnitRegistry()

def to_timestamp(datetime_object):
    return f"{datetime_object.year}-{datetime_object.month}-{datetime_object.day}"

def convert_weather_data(hourly_data, preferred_units):
    for index, value in preferred_units.items():
        hourly_data[index] = hourly_data[index].pint.to(value)
    return hourly_data

@st.cache_data(ttl=60)
def get_all_weather_data(location: str, start_date: str, end_date: str):
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

def translate_weather_code(code):
    translator = {
        0: 'Clear Sky',
        1: 'Mainly Clear',
        2: 'Partly Cloudy',
        3: 'Overcast',
        45: 'Fog',
        48: 'Depositing Rime Fog',
        51: 'Light Drizzle',
        53: 'Moderate Drizzle',
        55: 'Dense Drizzle',
        56: 'Light Freezing Drizzle',
        57: 'Dense Freezing Drizzle',
        61: 'Light Rain',
        63: 'Moderate Rain',
        65: 'Heavy Rain',
        66: 'Light Freezing Rain',
        67: 'Heavy Freezing Rain',
        71: 'Light Snow Fall',
        73: 'Moderate Snow Fall',
        75: 'Heavy Snow Fall',
        77: 'Snow Grains',
        80: 'Slight Rain Showers',
        81: 'Moderate Rain Showers',
        82: 'Violent Rain Showers',
        85: 'Slight Snow Showers',
        86: 'Heavy Snow Showers',
        95: 'Thunderstorms',
        96: 'Slight Thunderstorms with Hail',
        99: 'Heavy Thunderstorms with Hail'
    }

    code = int(code)
    if code not in translator.keys():
        raise RuntimeError(f'Weather Code {code} not found')
    
    return translator[code]

def translate_aqi(aqi):
    aqi = int(aqi)

    if aqi <= 50:
        result = 'Good'
    elif aqi <= 100:
        result = 'Moderate'
    elif aqi <= 150:
        result = 'Unhealthy for Sensitive Groups'
    elif aqi <= 200:
        result = 'Unhealthy'
    elif aqi <= 300:
        result = 'Very Unhealthy'
    elif aqi <= 500:
        result = 'Hazardous'
    else:
        result = 'AQI could not be processed'

    return result