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
import unified
import json

# set up tzwhere
tf = TimezoneFinder()

# set plotly as graphing backend
pd.options.plotting.backend = 'plotly'

# create geocoder
geocoder = utilities.generate_geocoder()

# create useful time deltas
day_delta = pd.Timedelta(1, 'day')
half_day_delta = pd.Timedelta(12, 'hours')
one_hour_delta = pd.Timedelta(1, 'hour')

# create tabs
tab1, tab2, tab3 = st.tabs(["Weather Overview", "Time-Series View", "Data Downloader"])

# select units
units = st.sidebar.selectbox('Units Preference',
                     ('Metric', 'Conventional'))

ureg = utilities.get_ureg()
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
        'direct_radiation': 'W/m**2',
        'direct_normal_irradiance': 'W/m**2',
        'diffuse_radiation': 'W/m**2'
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
        'direct_radiation': 'BTU/(hr*ft**2)',
        'direct_normal_irradiance': 'BTU/(hr*ft**2)',
        'diffuse_radiation': 'BTU/(hr*ft**2)'
    }
    temperature_string = "\N{DEGREE SIGN}F"
pint_pandas.PintType.ureg = ureg

# input location as address, zip code, etc.
location = st.sidebar.text_input('Location (Street Address, Zip Code, etc.):', 
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
st.sidebar.map(coordinates_df)

# determine useful times
current_time_utc = pd.Timestamp.utcnow()
current_date_utc = current_time_utc.floor('d')
tomorrow_date_utc = (current_time_utc + day_delta).floor('d')
tomorrow_time_utc = current_time_utc + day_delta
yesterday_time_utc = current_time_utc - day_delta
past_limit_utc = current_date_utc - 2 * day_delta
future_limit_utc = current_date_utc + 5 * day_delta

current_time_local = current_time_utc.tz_convert(tz=user_timezone)
current_date_local = current_time_local.floor('d')
tomorrow_date_local = (current_time_local + day_delta).floor('d')
past_limit_local = current_date_local - 1 * day_delta
future_limit_local = current_date_local + 4 * day_delta

with tab1:
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
    hourly_weather_data, hourly_weather_units, daily_weather_data, daily_weather_units = utilities.get_all_weather_data(
                                                f"{coordinates.latitude},{coordinates.longitude}",
                                                utilities.to_timestamp(current_date_utc),
                                                utilities.to_timestamp(tomorrow_date_utc))
    # daily_weather_data, daily_weather_units = utilities.get_daily_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
    #                                               utilities.to_timestamp(current_date_utc),
    #                                               utilities.to_timestamp(tomorrow_date_utc))
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

    # generate air quality information
    utilities.write_centered('Air Quality', header='h1')
    st.plotly_chart(utilities.create_aqi_plot(this_hour_data['us_aqi'].magnitude, 'AQI'))

    air_quality_subindices = [
        ["us_aqi_pm2_5", "PM 2.5"],
        ["us_aqi_pm10", "PM 10"],
        ["us_aqi_nitrogen_dioxide", "Nitrogen Dioxide"],
        ["us_aqi_ozone", "Ozone"],
        ["us_aqi_sulphur_dioxide", "Sulphur Dioxide"],
        ["us_aqi_carbon_monoxide", "Carbon Monoxide"]
    ]
    aqi_row1 = st.columns(3)
    aqi_row2 = st.columns(3)
    for col, (var, natural_name) in zip((aqi_row1 + aqi_row2), air_quality_subindices):
        tile = col.container(gap=None)
        tile.plotly_chart(utilities.create_aqi_plot(this_hour_data[var].magnitude, natural_name))

    with st.expander('Detailed Air Quality Data'):
        air_quality_info = pd.DataFrame([
            ["PM 2.5", f"{this_hour_data['pm2_5'].magnitude} {utilities.pretty_print_unit(this_hour_data['pm2_5'])}"],
            ["PM 10", f"{this_hour_data['pm10'].magnitude} {utilities.pretty_print_unit(this_hour_data['pm10'])}"],
            ["Nitrogen Dioxide", f"{this_hour_data['nitrogen_dioxide'].magnitude} {utilities.pretty_print_unit(this_hour_data['nitrogen_dioxide'])}"],
            ["Carbon Monoxide", f"{this_hour_data['carbon_monoxide'].magnitude} {utilities.pretty_print_unit(this_hour_data['carbon_monoxide'])}"],
            ["Ozone", f"{this_hour_data['ozone'].magnitude} {utilities.pretty_print_unit(this_hour_data['ozone'])}"],
            ["Sulphur Dioxide", f"{this_hour_data['sulphur_dioxide'].magnitude} {utilities.pretty_print_unit(this_hour_data['sulphur_dioxide'])}"],
            ["Carbon Dioxide", f"{this_hour_data['carbon_dioxide'].magnitude} {utilities.pretty_print_unit(this_hour_data['carbon_dioxide'])}"],
        ])
        st.dataframe(air_quality_info)

with tab2:

    # get all weather data for plotting
    hourly_weather_data, hourly_weather_units, _, _ = utilities.get_all_weather_data(
                                                f"{coordinates.latitude},{coordinates.longitude}",
                                                utilities.to_timestamp(past_limit_utc),
                                                utilities.to_timestamp(future_limit_utc))
    hourly_weather_data = utilities.convert_weather_data(hourly_weather_data, 
                                                         hourly_weather_units,
                                                         preferred_units,
                                                         user_timezone)
    
    filtered_weather_data = hourly_weather_data[(hourly_weather_data['timestamp_utc'] >= past_limit_local) & 
                                                (hourly_weather_data['timestamp_utc'] <= future_limit_local)]
    
    # create temperature plot
    temperature_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                            y=filtered_weather_data['temperature_2m'].pint.magnitude, 
                                            name='Temperature'))
    temperature_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['apparent_temperature'].pint.magnitude, 
                                          name='Apparent Temperature'))
    temperature_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['temperature_2m'].pint),
                                   title='Temperature')
    st.plotly_chart(temperature_plot)

    # create rain plot
    rain_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                         y=filtered_weather_data['precipitation'].pint.magnitude, 
                                         name='Precipitation',
                                         showlegend=True))
    rain_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['precipitation'].pint),
                            title='Precipitation')
    st.plotly_chart(rain_plot)

    # create snow plot
    rain_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                         y=filtered_weather_data['snowfall'].pint.magnitude, 
                                         name='Snowfall',
                                         showlegend=True))
    rain_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['snowfall'].pint),
                            title='Snowfall')
    st.plotly_chart(rain_plot)

    # create humidity plot
    humidity_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                         y=filtered_weather_data['relative_humidity_2m'].pint.magnitude, 
                                         name='Relative Humidity',
                                         showlegend=True))
    humidity_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['relative_humidity_2m'].pint),
                                title='Relative Humidity')
    st.plotly_chart(humidity_plot)

    # create pressure plot
    pressure_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                         y=filtered_weather_data['pressure_msl'].pint.magnitude, 
                                         name='Pressure',
                                         showlegend=True))
    pressure_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['pressure_msl'].pint),
                                title='Pressure')
    st.plotly_chart(pressure_plot)

    # create uv plot
    uv_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                            y=filtered_weather_data['direct_radiation'].pint.magnitude, 
                                            name='Direct Radiation'))
    uv_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['direct_normal_irradiance'].pint.magnitude, 
                                          name='Direct Normal Irradiance'))
    uv_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['diffuse_radiation'].pint.magnitude, 
                                          name='Diffuse Radiation'))
    uv_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['direct_radiation'].pint),
                          title='Solar Radiation')
    st.plotly_chart(uv_plot)

    # create air quality plot
    air_quality_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                            y=filtered_weather_data['pm2_5'].pint.magnitude, 
                                            name='PM 2.5'))
    air_quality_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['pm10'].pint.magnitude, 
                                          name='PM10'))
    air_quality_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['nitrogen_dioxide'].pint.magnitude, 
                                          name='Nitrogen Dioxide'))
    air_quality_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['carbon_monoxide'].pint.magnitude, 
                                          name='Carbon Monoxide'))
    air_quality_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['ozone'].pint.magnitude, 
                                          name='Ozone'))
    air_quality_plot.add_trace(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                          y=filtered_weather_data['sulphur_dioxide'].pint.magnitude, 
                                          name='Sulphur Dioxide'))
    air_quality_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['pm2_5'].pint),
                                   title='Air Quality')
    st.plotly_chart(air_quality_plot)

    # create carbon dioxide plot
    co2_plot = go.Figure(go.Scatter(x=filtered_weather_data['timestamp_utc'], 
                                         y=filtered_weather_data['carbon_dioxide'].pint.magnitude, 
                                         name='Carbon Dioxide',
                                         showlegend=True))
    co2_plot.update_layout(yaxis_title=utilities.pretty_print_unit(filtered_weather_data['carbon_dioxide'].pint),
                                title='Carbon Dioxide')
    st.plotly_chart(co2_plot)

with tab3:
    with st.form('download_form'):
        st.write('Hourly Data')
        hourly_checkboxes = [st.checkbox(i) for i in utilities.hourly_variables]

        st.write('Daily Data')
        daily_checkboxes = [st.checkbox(i) for i in utilities.daily_variables]

        submitted = st.form_submit_button("Fetch Data")

    if submitted:
        # get variables
        hourly_vars = []
        for i, truth in enumerate(hourly_checkboxes):
            if truth:
                hourly_vars.append(utilities.hourly_variables[i])

        daily_vars = []
        for i, truth in enumerate(daily_checkboxes):
            if truth:
                daily_vars.append(utilities.daily_variables[i])

        variables = ",".join(hourly_vars + daily_vars)
        location_str = f"{coordinates.latitude},{coordinates.longitude}"
        data = unified.fetch_unified(variables, 
                                     location_str,
                                     'both',
                                     utilities.to_timestamp(past_limit_utc),
                                     utilities.to_timestamp(future_limit_utc))
        
        st.download_button('Download Data', data=json.dumps(data, indent=4))

