from zoneinfo import ZoneInfo
import streamlit as st
from streamlit_javascript import st_javascript
from geopy.geocoders import Nominatim
import pandas as pd
import unified
import utilities
import plotly.graph_objects as go

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

weather_data = utilities.get_all_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
                                              utilities.to_timestamp(yesterday_time_utc),
                                              utilities.to_timestamp(tomorrow_time_utc))

# get pertinent weather data
time_window_min = current_time_utc - day_delta
time_window_max = current_time_utc + day_delta

weather_data_window = weather_data[(weather_data['timestamp_utc'] <= time_window_max) & 
                                   (weather_data['timestamp_utc'] >= time_window_min)]

time_data = weather_data_window['timestamp_utc'].dt.tz_convert(tz=user_timezone)
temp_data = weather_data_window['temperature_2m'].pint.to('degF').pint.magnitude

fig = go.Figure(go.Scatter(x=time_data, y=temp_data))
st.plotly_chart(fig)