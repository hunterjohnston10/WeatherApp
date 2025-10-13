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
import tempfile
import csv
import itertools

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

# plot location on map
st.sidebar.map(coordinates_df)

# determine useful times
current_time_utc = pd.Timestamp.utcnow()
current_date_utc = current_time_utc.floor('d')
tomorrow_date_utc = (current_time_utc + day_delta).floor('d')
yesterday_date_utc = (current_date_utc - day_delta)
tomorrow_time_utc = current_time_utc + day_delta
yesterday_time_utc = current_time_utc - day_delta
past_limit_utc = current_date_utc - 8 * day_delta
future_limit_utc = current_date_utc + 5 * day_delta

current_time_local = current_time_utc.tz_convert(tz=user_timezone)
current_date_local = current_time_local.floor('d')
tomorrow_date_local = (current_time_local + day_delta).floor('d')
past_limit_local = current_date_local - 7 * day_delta
future_limit_local = current_date_local + 3 * day_delta

with tab1:
    # get sun data
    sunrise_sunset_data = utilities.get_sunrise_sunset(f"{coordinates.latitude},{coordinates.longitude}",
                                                utilities.to_timestamp(yesterday_date_utc),
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
                                                utilities.to_timestamp(yesterday_date_utc),
                                                utilities.to_timestamp(tomorrow_date_utc))
    # daily_weather_data, daily_weather_units = utilities.get_daily_weather_data(f"{coordinates.latitude},{coordinates.longitude}",
    #                                               utilities.to_timestamp(current_date_utc),
    #                                               utilities.to_timestamp(tomorrow_date_utc))
    weather_data_daily = utilities.convert_weather_data(daily_weather_data, 
                                                        daily_weather_units, 
                                                        preferred_units,
                                                        tz=user_timezone)

    # separate weather data between today and tomorrow
    today_daily_weather = weather_data_daily[(weather_data_daily['date'] >= current_date_local) 
                            & (weather_data_daily['date'] < tomorrow_date_local)].iloc[0]

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

    this_hour_data = weather_data[(weather_data['timestamp_utc'] >= this_hour) & 
                                  (weather_data['timestamp_utc'] < next_hour)]

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
    temperature_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['temperature_2m', 'apparent_temperature'],
        weather_names=['Temperature', 'Apparent Temperature'],
        unit_name=utilities.pretty_print_unit(ureg(preferred_units['temperature_2m'])),
        title='Temperature',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(temperature_plot)

    # create rain plot
    rain_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['precipitation'],
        weather_names=['Precipitation'],
        unit_name=utilities.pretty_print_unit(ureg(preferred_units['precipitation'])),
        title='Precipitation',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(rain_plot)

    # create snow plot
    snow_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['snowfall'],
        weather_names=['Snowfall'],
        unit_name=utilities.pretty_print_unit(ureg(preferred_units['snowfall'])),
        title='Snowfall',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(snow_plot)

    # create humidity plot
    humidity_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['relative_humidity_2m'],
        weather_names=['Relative Humidity'],
        unit_name='%',
        title='Relative Humidity',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(humidity_plot)

    # create pressure plot
    pressure_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['pressure_msl'],
        weather_names=['Pressure'],
        unit_name=utilities.pretty_print_unit(ureg(preferred_units['pressure_msl'])),
        title='Pressure',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(pressure_plot)

    # create uv plot
    uv_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['direct_radiation', 'direct_normal_irradiance', 'diffuse_radiation'],
        weather_names=['Direct Radiation', 'Direct Normal Irradiance', 'Diffuse Radiation'],
        unit_name=utilities.pretty_print_unit(ureg(preferred_units['direct_radiation'])),
        title='Solar Radiation',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(uv_plot)

    # create air quality plot
    air_quality_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['pm2_5', 'pm10', 'nitrogen_dioxide', 'carbon_monoxide', 'ozone', 'sulphur_dioxide'],
        weather_names=['PM 2.5', 'PM 10', 'Nitrogen Dioxide', 'Carbon Monoxide', 'Ozone', 'Sulphur Dioxide'],
        unit_name=utilities.pretty_print_unit(filtered_weather_data['pm2_5'].pint),
        title='Air Quality',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(air_quality_plot)

    # create carbon dioxide plot
    co2_plot = utilities.create_forecast_plot(
        hourly_data=filtered_weather_data,
        weather_keys=['carbon_dioxide'],
        weather_names=['Carbon Dioxide'],
        unit_name=utilities.pretty_print_unit(filtered_weather_data['carbon_dioxide'].pint),
        title='Carbon Dioxide',
        current_time=current_time_local,
        future_time_limit=future_limit_local
    )
    st.plotly_chart(co2_plot)

with tab3:
    with st.form('download_form'):
        begin_date = st.date_input('Begin Date',
                                   value=utilities.to_timestamp(past_limit_utc),
                                   max_value=utilities.to_timestamp(future_limit_utc))
        end_date = st.date_input('End Date',
                                 value=utilities.to_timestamp(future_limit_utc),
                                 max_value=utilities.to_timestamp(future_limit_utc))
        
        format_radio = st.radio('Format',
                                options=['JSON', 'CSV', 'Parquet'],
                                horizontal=True,
                                width='stretch')

        st.write('Hourly Data')
        hourly_cols = itertools.cycle(st.columns(3))
        hourly_checkboxes = [next(hourly_cols).checkbox(i) for i in utilities.hourly_variables]

        st.write('Daily Data')
        daily_cols = itertools.cycle(st.columns(3))
        daily_checkboxes = [next(daily_cols).checkbox(i) for i in utilities.daily_variables]

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
                                     utilities.to_timestamp(begin_date),
                                     utilities.to_timestamp(end_date))
        hourly_data = data['data']['hourly']
        daily_data = data['data']['daily']
        units = data['units']

        # create hourly and daily pandas files
        try:
            hourly_keys = list(hourly_data[0].keys())
            hourly_units = {x: units['time'] if x=='timestamp_utc' else units[x] for x in hourly_keys}
            header_units = [x + '_units' for x in hourly_keys]
            header_row = list(itertools.chain(*zip(hourly_keys, header_units)))

            hd_pd = []
            for hd in hourly_data:
                ld = []
                for k in hourly_keys:
                    ld.append(hd[k])
                    ld.append(hourly_units[k])
                hd_pd.append(ld)
            hd_pd = pd.DataFrame(hd_pd, columns=header_row)
        except IndexError:
            hd_pd = pd.DataFrame()

        try:
            daily_keys = list(daily_data[0].keys())
            daily_units = {x: units['time'] if x=='date' else units[x] for x in daily_keys}
            header_units = [x + '_units' for x in daily_keys]
            header_row = list(itertools.chain(*zip(daily_keys, header_units)))

            dd_pd = []
            for hd in daily_data:
                ld = []
                for k in daily_keys:
                    ld.append(hd[k])
                    ld.append(daily_units[k])
                dd_pd.append(ld)
            dd_pd = pd.DataFrame(dd_pd, columns=header_row)
        except IndexError:
            dd_pd = pd.DataFrame()
        
        if format_radio == 'JSON':
            download_data = json.dumps(data, indent=4)
            download_fname = 'weather_data_download.json'
        
            st.download_button('Download Data', data=download_data, file_name=download_fname, on_click='ignore')
        elif format_radio == 'CSV':
            # generate houly data file
            st.download_button('Download Hourly Data', 
                               data=hd_pd.to_csv().encode('utf-8'), 
                               file_name='hourly_weather_data_download.csv', 
                               on_click='ignore')
            # generate daily data file
            st.download_button('Download Daily Data', 
                               data=dd_pd.to_csv().encode('utf-8'), 
                               file_name='daily_weather_data_download.csv', 
                               on_click='ignore')
        elif format_radio == 'Parquet':
            # generate houly data file
            st.download_button('Download Hourly Data', 
                               data=hd_pd.to_parquet(), 
                               file_name='hourly_weather_data_download.parquet', 
                               on_click='ignore')
            # generate daily data file
            st.download_button('Download Daily Data', 
                               data=dd_pd.to_parquet(), 
                               file_name='daily_weather_data_download.parquet', 
                               on_click='ignore')

