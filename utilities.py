import unified
import pandas as pd
import pint
import streamlit as st
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
import pint_pandas
import geocoder
from types import SimpleNamespace

# define useful constants
hourly_variables = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "precipitation_probability",
    "snowfall",
    "pressure_msl",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "visibility",
    "cloud_cover",
    "evapotranspiration",
    "weather_code",
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "carbon_monoxide",
    "ozone",
    "sulphur_dioxide",
    "carbon_dioxide",
    "us_aqi",
    "us_aqi_pm2_5",
    "us_aqi_pm10",
    "us_aqi_nitrogen_dioxide",
    "us_aqi_ozone",
    "us_aqi_sulphur_dioxide",
    "us_aqi_carbon_monoxide",
    "direct_radiation",
    "direct_normal_irradiance",
    "diffuse_radiation"]
daily_variables = [
    'uv_index_max',
    'temperature_2m_max',
    'temperature_2m_min',
    'apparent_temperature_max',
    'apparent_temperature_min',
    'precipitation_sum',
    'rain_sum',
    'showers_sum',
    'snowfall_sum',
    'precipitation_hours',
    'precipitation_probability_max',
    'precipitation_probability_mean',
    'precipitation_probability_min',
    'weather_code_daily',
    'wind_speed_10m_max',
    'wind_gusts_10m_max',
    'wind_direction_10m_dominant'
    ]

@st.cache_resource(ttl=86400) # 1 day cache
def generate_geocoder():
    return geocoder.arcgis

@st.cache_data(ttl=86400) # 1 day cache
def get_location(location, _geocoder):
    latlng = _geocoder(location).latlng
    return SimpleNamespace(latitude=latlng[0], longitude=latlng[1])

@st.cache_resource(ttl=900) # 15 minute cache
def get_ureg():
    ureg = pint.UnitRegistry()
    ureg.load_definitions('weather_units.txt')
    return ureg

def to_timestamp(datetime_object):
    return f"{datetime_object.year:04d}-{datetime_object.month:02d}-{datetime_object.day:02d}"

def to_12_hr_format(datetime_object):
    return datetime_object.strftime("%I:%M:%S %p")

def write_centered(content, header='span'):
    return st.markdown(f"<{header} style='text-align: center'>{content}</{header}>", unsafe_allow_html=True)

def write_left(content, header='span'):
    return st.markdown(f"<{header} style='text-align: left'>{content}</{header}>", unsafe_allow_html=True)

def write_right(content, header='span'):
    return st.markdown(f"<{header} style='text-align: right'>{content}</{header}>", unsafe_allow_html=True)

def pretty_print_unit(quantity):
    return f"{quantity.units:~#P}"

def degree_to_compass(num):
    val=int((num/22.5)+.5)
    arr=["N","NNE","NE","ENE","E","ESE", "SE", "SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return arr[(val % 16)]

def convert_weather_data(input_weather_data, weather_units, preferred_units, tz=None):
    ureg = get_ureg()
    pint_pandas.PintType.ureg = ureg
    
    weather_data = input_weather_data.copy()

    if tz is not None:
        try:
            weather_data['timestamp_utc'] = weather_data['timestamp_utc'].dt.tz_convert(tz=tz)
        except KeyError:
            pass

        try:
            weather_data['date'] = weather_data['date'].dt.tz_convert(tz=tz)
        except KeyError:
            pass
    
    # convert all data to pint units
    for column in weather_data.columns:
        try:
            weather_data[column] = weather_data[column].astype(f"pint[{weather_units[column]}]")
        except KeyError:
            continue
    for index, value in preferred_units.items():
        try:
            weather_data[index] = weather_data[index].pint.to(value)
        except KeyError:
            continue
    return weather_data

def get_sunrise_sunset(location: str, start_date: str, end_date: str):
    # get sunrise data
    data = unified.fetch_unified('sunrise', 
                                location,
                                'both',
                                start_date,
                                end_date)
    api_var = unified.VARIABLES['sunrise'].api_var_name
    sunrise_data = pd.DataFrame.from_dict(data['data']['daily'])
    sunrise_data = sunrise_data.rename(columns={api_var: 'sunrise'})
    sunrise_data['date'] = pd.to_datetime(sunrise_data['date']).dt.tz_localize('UTC')
    sunrise_data['sunrise'] = pd.to_datetime(sunrise_data['sunrise']).dt.tz_localize('UTC')

    # get sunset data
    data = unified.fetch_unified('sunset', 
                                location,
                                'both',
                                start_date,
                                end_date)
    api_var = unified.VARIABLES['sunset'].api_var_name
    sunset_data = pd.DataFrame.from_dict(data['data']['daily'])
    sunset_data = sunset_data.rename(columns={api_var: 'sunset'})
    sunset_data['date'] = pd.to_datetime(sunset_data['date']).dt.tz_localize('UTC')
    sunset_data['sunset'] = pd.to_datetime(sunset_data['sunset']).dt.tz_localize('UTC')

    all_data = sunrise_data.merge(sunset_data, how='outer', on='date')

    return all_data

@st.cache_data(ttl=900) # 15 minute cache
def get_daily_weather_data(location, start_date, end_date):    
    daily_data = pd.DataFrame()
    daily_units = {}
    
    for variable in daily_variables:
        data = unified.fetch_unified(variable, 
                                     location,
                                     'both',
                                     start_date,
                                     end_date)
        
        api_var = unified.VARIABLES[variable].api_var_name
        units = data['units'][api_var].strip().replace(' ', '_')
        daily_units[variable] = units

        parsed_data = pd.DataFrame.from_dict(data['data'])
        parsed_data = parsed_data.rename(columns={api_var: variable})
        parsed_data['date'] = pd.to_datetime(parsed_data['date']).dt.tz_localize('UTC')

        if daily_data.empty:
            daily_data = parsed_data
        else:
            daily_data = daily_data.merge(parsed_data, how='outer', on='date')

    return daily_data, daily_units
    
@st.cache_data(ttl=900)  # 15 minute cache
def get_hourly_weather_data(location, start_date, end_date):
    hourly_data = pd.DataFrame()
    hourly_units = {}

    for variable in hourly_variables:
        data = unified.fetch_unified(variable, 
                                     location,
                                     'both',
                                     start_date,
                                     end_date)
        
        api_var = unified.VARIABLES[variable].api_var_name
        units = data['units'][api_var].strip().replace(' ', '_')
        hourly_units[variable] = units

        parsed_data = pd.DataFrame.from_dict(data['data'])
        parsed_data = parsed_data.rename(columns={api_var: variable})
        parsed_data['timestamp_utc'] = pd.to_datetime(parsed_data['timestamp_utc']).dt.tz_localize('UTC')

        if hourly_data.empty:
            hourly_data = parsed_data
        else:
            hourly_data = hourly_data.merge(parsed_data, how='outer', on='timestamp_utc')

    return hourly_data, hourly_units

@st.cache_data(ttl=900)  # 15 minute cache
def get_all_weather_data(location: str, start_date: str, end_date: str):
    

    hourly_data = pd.DataFrame()
    hourly_units = {}
    daily_data = pd.DataFrame()
    daily_units = {}

    data = unified.fetch_unified(','.join(hourly_variables + daily_variables), 
                                    location,
                                    'both',
                                    start_date,
                                    end_date)
    
    for variable in hourly_variables:
        api_var = unified.VARIABLES[variable].api_var_name
        units = data['units'][api_var].strip().replace(' ', '_')
        hourly_units[variable] = units

    hourly_data = pd.DataFrame.from_dict(data['data']['hourly'])
    hourly_data = hourly_data.rename(columns={unified.VARIABLES[variable].api_var_name: variable for variable in hourly_variables})
    hourly_data['timestamp_utc'] = pd.to_datetime(hourly_data['timestamp_utc']).dt.tz_localize('UTC')
    
    for variable in daily_variables:
        api_var = unified.VARIABLES[variable].api_var_name
        units = data['units'][api_var].strip().replace(' ', '_')
        daily_units[variable] = units

    daily_data = pd.DataFrame.from_dict(data['data']['daily'])
    daily_data = daily_data.rename(columns={unified.VARIABLES[variable].api_var_name: variable for variable in daily_variables})
    daily_data['date'] = pd.to_datetime(daily_data['date']).dt.tz_localize('UTC')
    
    return hourly_data, hourly_units, daily_data, daily_units

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

def generate_daily_summary(day_data):
    temperature_unit = pretty_print_unit(day_data['temperature_2m_max'])
    rain_unit = pretty_print_unit(day_data['rain_sum'])
    snow_unit = pretty_print_unit(day_data['snowfall_sum'])
    wind_unit = pretty_print_unit(day_data['wind_speed_10m_max'])

    container = st.container()
    with container:
        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "High/Low",
                    f'{day_data["temperature_2m_max"].magnitude:.1f}/{day_data["temperature_2m_min"].magnitude:.1f} {temperature_unit}',
                    width='content'
                )

            with c2:
                st.metric(
                    "Apparent High/Low",
                    f'{day_data["apparent_temperature_max"].magnitude:.1f}/{day_data["apparent_temperature_min"].magnitude:.1f} {temperature_unit}',
                    width='content'
                )

            with c3:
                st.metric(
                    "Precip. Prob. High/Mean/Low",
                    f'{day_data["precipitation_probability_max"].magnitude:.0f}/{day_data["precipitation_probability_mean"].magnitude:.0f}/{day_data["precipitation_probability_min"].magnitude:.0f} %',
                    width='content'
                )

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Precipitation",
                    f"{day_data['precipitation_sum'].magnitude:.2f} {rain_unit}",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Snow",
                    f"{day_data['snowfall_sum'].magnitude:.2f} {snow_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "UV Index",
                    f"{day_data['uv_index_max'].magnitude:.1f}",
                    width='content'
                )   

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Max Windspeed",
                    f"{day_data['wind_speed_10m_max'].magnitude:.1f} {wind_unit}",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Max Gust",
                    f"{day_data['wind_gusts_10m_max'].magnitude:.1f} {wind_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "Dominant Wind Direction",
                    f"{degree_to_compass(day_data['wind_direction_10m_dominant'].magnitude)}",
                    width='content'
                )   

    return container

def generate_current_summary(current_data):
    temperature_unit = pretty_print_unit(current_data['temperature_2m'])
    rain_unit = pretty_print_unit(current_data['precipitation'])
    snow_unit = pretty_print_unit(current_data['snowfall'])
    wind_unit = pretty_print_unit(current_data['wind_speed_10m'])
    visibility_unit = pretty_print_unit(current_data['visibility'])
    pressure_unit = pretty_print_unit(current_data['pressure_msl'])
    solar_unit = pretty_print_unit(current_data['direct_radiation'])

    container = st.container()
    with container:
        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Current Temperature",
                    f'{current_data["temperature_2m"].magnitude:.1f} {temperature_unit}',
                    width='content'
                )

            with c2:
                st.metric(
                    "Apparent Temperature",
                    f'{current_data["apparent_temperature"].magnitude:.1f} {temperature_unit}',
                    width='content'
                )

            with c3:
                st.metric(
                    "Precip. Prob.",
                    f'{current_data["precipitation_probability"].magnitude:.0f} %',
                    width='content'
                )

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Precipitation",
                    f"{current_data['precipitation'].magnitude:.2f} {rain_unit}",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Snow",
                    f"{current_data['snowfall'].magnitude:.2f} {snow_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "Visibility",
                    f"{current_data['visibility'].magnitude:.1f} {visibility_unit}",
                    width='content'
                )   

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Windspeed",
                    f"{current_data['wind_speed_10m'].magnitude:.1f} {wind_unit}",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Gusts",
                    f"{current_data['wind_gusts_10m'].magnitude:.1f} {wind_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "Wind Direction",
                    f"{degree_to_compass(current_data['wind_direction_10m'].magnitude)}",
                    width='content'
                ) 

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Relative Humidity",
                    f"{current_data['relative_humidity_2m'].magnitude:.1f} %",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Pressure",
                    f"{current_data['pressure_msl'].magnitude:.1f} {pressure_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "Cloud Cover",
                    f"{current_data['cloud_cover'].magnitude:.1f} %",
                    width='content'
                )

        with st.container(horizontal=True, gap='small'):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(
                    "Direct Radiation",
                    f"{current_data['direct_radiation'].magnitude:.1f} {solar_unit}",
                    width='content'
                )  

            with c2:
                st.metric(
                    "Direct Normal Irradiance",
                    f"{current_data['direct_normal_irradiance'].magnitude:.1f} {solar_unit}",
                    width='content'
                )     

            with c3:
                st.metric(
                    "Diffuse Radiation",
                    f"{current_data['diffuse_radiation'].magnitude:.1f} {solar_unit}",
                    width='content'
                )    

    return container

def create_aqi_plot(aqi_data, title):

    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = aqi_data,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': title, 'font': {'size': 24}},
        gauge = {
            'axis': {'range': [None, 500], 'tickwidth': 1, 'tickcolor': "black"},
            'bar': {'color': "darkblue"},
            'bgcolor': "white",
            'borderwidth': 2,
            'bordercolor': "gray",
            'steps': [
                {'range': [0, 50], 'color': 'green'},
                {'range': [50, 100], 'color': 'yellow'},
                {'range': [100, 150], 'color': 'orange'},
                {'range': [150, 200], 'color': 'red'},
                {'range': [200, 300], 'color': 'magenta'},
                {'range': [300, 500], 'color': 'brown'},],
            }))

    fig.update_layout(font = {'color': "black", 'family': "Arial"})

    return fig

def create_forecast_plot(hourly_data, weather_keys, weather_names, unit_name, title, current_time, future_time_limit):
    plot = go.Figure()
    for k, n in zip(weather_keys, weather_names):
        plot.add_trace(go.Scatter(x=hourly_data['timestamp_utc'],
                                 y=hourly_data[k].pint.magnitude,
                                 name=n,
                                 showlegend=True))
        
    plot.add_vrect(
        x0=current_time.floor('h'),
        x1=future_time_limit,
        fillcolor='gray',
        opacity=0.3,
        line_width=0
    )

    plot.update_layout(yaxis_title=unit_name, title=title)
    return plot