from zoneinfo import ZoneInfo
import streamlit as st
import streamlit.components.v1 as components
from streamlit_javascript import st_javascript
import pandas as pd
import utilities
import plotly.graph_objects as go
import pint
import pint_pandas
from timezonefinder import TimezoneFinder

# set up tzwhere
tf = TimezoneFinder()

# set plotly as graphing backend
pd.options.plotting.backend = 'plotly'

# create Nominatim geocoder
geocoder = utilities.generate_geocoder()

# create useful time deltas
day_delta = pd.Timedelta(1, 'day')
half_day_delta = pd.Timedelta(12, 'hours')
one_hour_delta = pd.Timedelta(1, 'hour')

# select units
units = st.selectbox('Units Preference',
                     ('Metric', 'Conventional'))

ureg = pint.UnitRegistry(autoconvert_to_preferred=True)
ureg.load_definitions('weather_units.txt')
if units == 'Metric':
    preferred_units = {
        'temperature_2m': 'degC',
        'precipitation': 'mm',
        'pressure_msl': 'hPa',
        'wind_speed_10m': 'kph',
        'apparent_temperature': 'degC',
        'snowfall': 'cm',
        'wind_gusts_10m': 'kph',
        'visibility': 'kilometers',
        'evapotranspiration': 'mm',
        'temperature_2m_max': 'degC',
        'temperature_2m_min': 'degC',
        'apparent_temperature_max': 'degC',
        'apparent_temperature_min': 'degC',
        'precipitation_sum': 'mm',
        'rain_sum': 'mm',
        'showers_sum': 'mm',
        'snowfall_sum': 'cm',
        'wind_speed_10m_max': 'kph',
        'wind_gusts_10m_max': 'kph',
    }
    temperature_string =  "\N{DEGREE SIGN}C"
else:
    preferred_units = {
        'temperature_2m': 'degF',
        'precipitation': 'in',
        'pressure_msl': 'inHg',
        'wind_speed_10m': 'mph',
        'apparent_temperature': 'degF',
        'snowfall': 'in',
        'wind_gusts_10m': 'mph',
        'visibility': 'miles',
        'evapotranspiration': 'in',
        'temperature_2m_max': 'degF',
        'temperature_2m_min': 'degF',
        'apparent_temperature_max': 'degF',
        'apparent_temperature_min': 'degF',
        'precipitation_sum': 'in',
        'rain_sum': 'in',
        'showers_sum': 'in',
        'snowfall_sum': 'in',
        'wind_speed_10m_max': 'mph',
        'wind_gusts_10m_max': 'mph',
    }
    temperature_string = "\N{DEGREE SIGN}F"
ureg.default_preferred_units = preferred_units
pint_pandas.PintType.ureg = ureg

# input location as address, zip code, etc.
location = st.text_input('Location (Street Address, Zip Code, etc.):', 
                         value="275 Ferst Dr NW, Atlanta, GA 30313", 
                         autocomplete="street-address postal-code address-level2", 
                         icon=':material/home:',
                         label_visibility='hidden')

# decode input location information
coordinates = utilities.get_location(location, geocoder)
coordinates_df = pd.DataFrame([[coordinates.latitude, coordinates.longitude]], columns=['LAT', 'LON'])

# get time zone from coordinates
user_timezone = tf.timezone_at(lng=coordinates.longitude, lat=coordinates.latitude)
print(user_timezone)

# plot location on map
st.map(coordinates_df)

# determine useful times
current_time_utc = pd.Timestamp.utcnow()
current_date_utc = current_time_utc.floor('d')
tomorrow_date_utc = (current_time_utc + day_delta).floor('d')
tomorrow_time_utc = current_time_utc + day_delta
yesterday_time_utc = current_time_utc - day_delta

current_time_local = current_time_utc.tz_convert(tz=user_timezone)
current_date_local = current_time_local.floor('d')
tomorrow_date_local = (current_time_local + day_delta).floor('d')

# get sun data
sunrise_sunset_data = utilities.get_sunrise_sunset(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(current_date_utc),
                                              utilities.to_timestamp(tomorrow_date_utc))

# get sunrise and sunset times for current local time
sunrise_data = sunrise_sunset_data['sunrise']
sunrise_time = sunrise_data[(sunrise_data >= current_date_local) 
                            & (sunrise_data < tomorrow_date_local)].dt.tz_convert(tz=user_timezone).iloc[0]

sunset_data = sunrise_sunset_data['sunset']
sunset_time = sunset_data[(sunset_data >= current_date_local) 
                          & (sunset_data < tomorrow_date_local)].dt.tz_convert(tz=user_timezone).iloc[0]

# Display sunrise and sunset information
sunrise_col, sunset_col = st.columns(2)

with sunrise_col:
    utilities.write_centered("🌅 Sunrise", header='h1')
    utilities.write_centered(utilities.to_12_hr_format(sunrise_time), header='p')

with sunset_col:
    utilities.write_centered('🌇 Sunset', header='h1')
    utilities.write_centered(utilities.to_12_hr_format(sunset_time), header='p')

# get daily weather data
daily_weather_data, daily_weather_units = utilities.get_daily_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(current_date_utc),
                                              utilities.to_timestamp(tomorrow_date_utc))
weather_data_daily = utilities.convert_weather_data(daily_weather_data, 
                                                    daily_weather_units, 
                                                    preferred_units,
                                                    tz=user_timezone)

# separate weather data between today and tomorrow
today_daily_weather = weather_data_daily.iloc[0, :]

# display weather for today
utilities.write_centered('Today Overview', header='h1')
utilities.write_centered(
    f"Today's forecast is {utilities.translate_weather_code(today_daily_weather['weather_code_daily'].magnitude)}",
    header='h2')


utilities.generate_daily_summary(today_daily_weather)

# get hourly weather data
hourly_weather_data, hourly_weather_units = utilities.get_hourly_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(yesterday_time_utc),
                                              utilities.to_timestamp(tomorrow_time_utc))
weather_data = utilities.convert_weather_data(hourly_weather_data, 
                                              hourly_weather_units, 
                                              preferred_units,
                                              tz=user_timezone)

this_hour = current_time_local.floor('h')
next_hour = current_time_local.ceil('h')

this_hour_data = weather_data[(weather_data['timestamp_utc'] == this_hour)]

# weird stuff is happening with this slice, so enforce a pandas series
if len(this_hour_data) > 1:
    raise RuntimeError("Error in retrieving this hour's data")
else:
    this_hour_data = this_hour_data.T.squeeze()

utilities.write_centered('Right Now Overview', header='h1')
utilities.write_centered(
    f"The current conditions are {utilities.translate_weather_code(this_hour_data['weather_code'].magnitude)}",
    header='h2')

utilities.generate_current_summary(this_hour_data)
st.plotly_chart(utilities.create_aqi_plot(this_hour_data['us_aqi'].magnitude, 'AQI'))