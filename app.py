from zoneinfo import ZoneInfo
import streamlit as st
from streamlit_javascript import st_javascript
from geopy.geocoders import Nominatim
import pandas as pd
import utilities
import plotly.graph_objects as go
import pint
import pint_pandas

# set plotly as graphing backend
pd.options.plotting.backend = 'plotly'

# get useful user information
user_timezone = ZoneInfo(st_javascript("""await (async () => {
            const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
            console.log(userTimezone)
            return userTimezone
})().then(returnValue => returnValue)"""))

# create Nominatim geocoder
geocoder = Nominatim(user_agent='ASDL-Weather-App')

# create useful time deltas
day_delta = pd.Timedelta(1, 'day')
half_day_delta = pd.Timedelta(12, 'hours')

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
coordinates = geocoder.geocode(location)
coordinates_df = pd.DataFrame([[coordinates.latitude, coordinates.longitude]], columns=['LAT', 'LON'])

# plot location on map
st.map(coordinates_df)

# get weather data
current_time_utc = pd.Timestamp.utcnow()
tomorrow_time_utc = current_time_utc + day_delta
yesterday_time_utc = current_time_utc - day_delta

sunrise_sunset_data = utilities.get_sunrise_sunset(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(yesterday_time_utc),
                                              utilities.to_timestamp(tomorrow_time_utc))

hourly_weather_data, hourly_weather_units, daily_weather_data, daily_weather_units = utilities.get_all_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(yesterday_time_utc),
                                              utilities.to_timestamp(tomorrow_time_utc))
weather_data = utilities.convert_weather_data(hourly_weather_data, hourly_weather_units, preferred_units)



# get pertinent weather data
time_window_min = current_time_utc - day_delta
time_window_max = current_time_utc + day_delta

weather_data_window = weather_data[(weather_data['timestamp_utc'] <= time_window_max) & 
                                   (weather_data['timestamp_utc'] >= time_window_min)]

time_data = weather_data_window['timestamp_utc'].dt.tz_convert(tz=user_timezone)
temp_data = weather_data_window['temperature_2m'].pint.magnitude

fig = go.Figure(go.Scatter(x=time_data, y=temp_data))
fig.update_layout(yaxis_title=temperature_string)
st.plotly_chart(fig)