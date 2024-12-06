from dotenv import load_dotenv
import os
import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import datetime, timedelta
from meteoalertapi import Meteoalert
import pandas as pd
import plotly.express as px
import json
# Load environment variables
def configure():
    """Load API key from .env file."""
    load_dotenv()

configure()
api_key = os.getenv('api_key')

# Fallback to st.secrets if .env configuration does not provide an API key
if not api_key:
    api_key = st.secrets.get('api_key')

# Weather API setup
location = "Eindhoven"

# Fetch forecast data
url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
response = requests.get(url)
data = response.json()

def fetch_tunnel_data():
    url = "https://data.eindhoven.nl/api/explore/v2.1/catalog/datasets/tunnelvisie-punten/records?limit=71"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['results']
    else:
        return []
    
def get_precipitation(lat, lon):
    """
    Fetches precipitation data for a given latitude and longitude.
    Args:
        lat (float): Latitude.
        lon (float): Longitude.
    Returns:
        tuple: Max precipitation intensity in mm/h and description.
    """
    try:
        response = requests.get(f"https://gps.buienradar.nl/getrr.php?lat={lat}&lon={lon}")
        if response.status_code == 200:
            raw_data = response.text.strip()
            return parse_precipitation(raw_data)
        else:
            return 0, "N/A"
    except Exception as e:
        return 0, "Error"

def parse_precipitation(data):
    """
    Parses precipitation data using the provided formula.
    Args:
        data (str): Precipitation data in the Buienradar format.
    Returns:
        tuple: Max precipitation intensity in mm/h and description.
    """
    lines = data.splitlines()
    intensities = []

    for line in lines:
        parts = line.split("|")
        if len(parts) == 2:
            try:
                waarde = int(parts[0])  # Extract the intensity value
                intensity = 10 ** ((waarde - 109) / 32)  # Apply the formula
                intensities.append(intensity)
            except ValueError:
                continue

    # Summarize
    max_intensity = max(intensities) if intensities else 0
    if max_intensity < 0.1:
        return max_intensity, "No rain"
    elif max_intensity <= 2.5:
        return max_intensity, "Light rain"
    elif max_intensity <= 7.5:
        return max_intensity, "Moderate rain"
    else:
        return max_intensity, "Heavy rain"

# Function to determine marker color based on precipitation level
def get_marker_color(precipitation_description):
    """
    Maps precipitation description to marker color.
    Args:
        precipitation_description (str): Description of precipitation.
    Returns:
        str: Marker color.
    """
    if precipitation_description == "No rain":
        return "green"
    elif precipitation_description == "Light rain":
        return "blue"
    elif precipitation_description == "Moderate rain":
        return "orange"
    elif precipitation_description == "Heavy rain":
        return "red"
    else:
        return "gray" 
    

# Check if 'forecast' exists in the response
if 'forecast' not in data or 'forecastday' not in data['forecast']:
    st.error("Invalid response: 'forecast' data is missing")
    st.stop()

# Retrieve historical data for the past 7 days
days_to_retrieve = 7
dates = []
daily_precipitation = []
for i in range(days_to_retrieve - 1, -1, -1):  # Start from 7 days ago to today
    date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
    history_url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date}"
    history_response = requests.get(history_url)
    if history_response.status_code == 200:
        history_data = history_response.json()
        dates.append(date)
        daily_precipitation.append(history_data['forecast']['forecastday'][0]['day']['totalprecip_mm'])
    else:
        st.warning(f"Failed to retrieve data for {date}")

# Extract current day's forecast
current_day_forecast = data['forecast']['forecastday'][0]
hourly_data_today = current_day_forecast['hour']
temperatures_today = [hour['temp_c'] for hour in hourly_data_today]
feels_like_today = [hour['feelslike_c'] for hour in hourly_data_today]
rainfall_today = [hour['precip_mm'] for hour in hourly_data_today]
humidity_today = [hour['humidity'] for hour in hourly_data_today]
times_today = [datetime.strptime(hour['time'], '%Y-%m-%d %H:%M').strftime('%H:%M') for hour in hourly_data_today]

# Find lowest and highest temperatures for current day
min_temp_today = min(temperatures_today)
max_temp_today = max(temperatures_today)
min_temp_time_today = times_today[temperatures_today.index(min_temp_today)]
max_temp_time_today = times_today[temperatures_today.index(max_temp_today)]

max_humidity = max(humidity_today)
min_humidity = min(humidity_today)
max_time = times_today[humidity_today.index(max_humidity)]
min_time = times_today[humidity_today.index(min_humidity)]

# Extract next day's forecast
next_day_forecast = data['forecast']['forecastday'][1]
hourly_data_tomorrow = next_day_forecast['hour']
temperatures_tomorrow = [hour['temp_c'] for hour in hourly_data_tomorrow]
feels_like_tomorrow = [hour['feelslike_c'] for hour in hourly_data_tomorrow]
precipitation = [hour['precip_mm'] for hour in hourly_data_tomorrow]
humidity = [hour['humidity'] for hour in hourly_data_tomorrow]
wind_speed = [hour['wind_kph'] for hour in hourly_data_tomorrow]
times_tomorrow = [datetime.strptime(hour['time'], '%Y-%m-%d %H:%M').strftime('%H:%M') for hour in hourly_data_tomorrow]

# Prepare data for the table
forecasted_dates = [day['date'] for day in data['forecast']['forecastday']]
forecasted_rainfall = [day['day']['totalprecip_mm'] for day in data['forecast']['forecastday']]

# Calculate averages for current day
avg_temp_today = sum(temperatures_today) / len(temperatures_today)
avg_feels_like_today = sum(feels_like_today) / len(feels_like_today)

# Calculate total rainfall
total_rainfall_today = sum(rainfall_today)
max_rainfall_today = max(rainfall_today)
peak_rainfall_time_today = times_today[rainfall_today.index(max_rainfall_today)]

# Clothing recommendations based on temperature and rainfall
if total_rainfall_today > 0.5:
    if avg_temp_today < 7:
        suggestion = "Take your gloves and umbrella!"
    elif avg_temp_today < 13:
        suggestion = "Bring your hat and umbrella!"
    else:
        suggestion = "Take an umbrella!"
else:
    if avg_temp_today < 7:
        suggestion = "Take your gloves!"
    elif avg_temp_today < 13:
        suggestion = "Bring your hat!"
    else:
        suggestion = "No special clothing needed today."

# Generate charts
# Temperature Chart
temp_chart = go.Figure()
temp_chart.add_trace(go.Scatter(x=times_today, y=temperatures_today, mode='lines+markers', name='Temperature (°C)', line=dict(color='blue')))
temp_chart.add_trace(go.Scatter(x=times_today, y=feels_like_today, mode='lines+markers', name='Feels Like (°C)', line=dict(color='orange')))





# Add annotations for min and max temperatures
temp_chart.add_trace(go.Scatter(
    x=[min_temp_time_today],
    y=[min_temp_today],
    mode='markers',
    marker=dict(color='red', size=10),
    showlegend=False  # Prevent from appearing as a separate trace
))
temp_chart.add_trace(go.Scatter(
    x=[max_temp_time_today],
    y=[max_temp_today],
    mode='markers',
    marker=dict(color='green', size=10),
    showlegend=False  # Prevent from appearing as a separate trace
))

# Add a custom legend for highest and lowest temperatures
temp_chart.add_trace(go.Scatter(
    x=[None], y=[None], mode='markers',
    marker=dict(color='red', size=10),
    name='Lowest Temp'
))
temp_chart.add_trace(go.Scatter(
    x=[None], y=[None], mode='markers',
    marker=dict(color='green', size=10),
    name='Highest Temp'
))

temp_chart.update_layout(
    title="Temperature Trend Throughout the Day (Today)",
    xaxis_title="Time",
    yaxis_title="Temperature (°C)",
    legend_title="Legend",
    template="plotly_white",
)

# Humidity Trend Chart
hum_chart = go.Figure()
hum_chart.add_trace(go.Scatter(x=times_today, y=humidity_today, mode='lines+markers', name='Humidity (%)', line=dict(color='blue')))
hum_chart.update_layout(
    title="Humidity Trend Throughout the Day (Today)",
    xaxis_title="Time",
    yaxis_title="Humidity (%)",
    legend_title="Legend",
    template="plotly_white"
)

hum_chart.add_trace(go.Scatter(
    x=[max_time],
    y=[max_humidity],
    mode='markers',
    marker=dict(color='green', size=10),
    name='Highest Humidity'
))
hum_chart.add_trace(go.Scatter(
    x=[min_time],
    y=[min_humidity],
    mode='markers',
    marker=dict(color='red', size=10),
    name='Lowest Humidity'
))

hum_chart.update_layout(
    title="Humidity Trend Throughout the Day (Today)",
    xaxis_title="Time",
    yaxis_title="Humidity %",
    legend_title="Legend",
    template="plotly_white",
)


# Rainfall Trend Chart for Today
rain_chart = go.Figure()
rain_chart.add_trace(go.Scatter(x=times_today, y=rainfall_today, mode='lines', fill='tozeroy', name='Rainfall (mm)', line=dict(color='blue')))
# Add horizontal lines for rainfall levels
rain_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
rain_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
rain_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")
rain_chart.update_layout(
    title="Rainfall Trend Throughout the Day (Today)",
    xaxis_title="Time",
    yaxis_title="Rainfall (mm)",
    template="plotly_white"
)

# Historical Precipitation Chart
historical_chart = go.Figure()
historical_chart.add_trace(go.Scatter(x=dates, y=daily_precipitation, mode='lines+markers', fill='tozeroy', name='Daily Precipitation (mm)'))
# Add horizontal lines for rainfall levels
historical_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
historical_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
historical_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")
historical_chart.update_layout(
    title="Precipitation Trends Over the Week",
    xaxis_title="Date",
    yaxis_title="Precipitation (mm)",
    template="plotly_white"
)

# Forecasted Rainfall Chart
forecast_rain_chart = go.Figure()
forecast_rain_chart.add_trace(go.Scatter(x=forecasted_dates, y=forecasted_rainfall, mode='lines+markers', fill='tozeroy', name='Forecasted Rainfall (mm)'))
# Add horizontal lines for rainfall levels
forecast_rain_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
forecast_rain_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
forecast_rain_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")
forecast_rain_chart.update_layout(
    title="Forecasted Rainfall Trends for Upcoming Days",
    xaxis_title="Date",
    yaxis_title="Rainfall (mm)",
    template="plotly_white"
)


tunnel_data = fetch_tunnel_data()

# Prepare data for Plotly map
tunnels = []
precipitation_data = []

for tunnel in tunnel_data:
    lat = float(tunnel['lat'])
    lon = float(tunnel['lon'])
    locatienaam = tunnel['locatienaam']
    jaar = tunnel['jaar']


    # Fetch precipitation data
    max_intensity, precipitation_description = get_precipitation(lat, lon)

    # Prepare the data
    tunnels.append({
        'locatienaam': locatienaam,
        'jaar': jaar,
        'lat': lat,
        'lon': lon,
        'precipitation_description': precipitation_description,
        'precipitation_intensity': max_intensity
    })

# Convert data to a DataFrame
df_tunnels = pd.DataFrame(tunnels)

# Create a Plotly map
fig = px.scatter_mapbox(df_tunnels,
                        lat=df_tunnels['lat'], lon=df_tunnels['lon'],
                        hover_name="locatienaam",
                        hover_data=["precipitation_description", "precipitation_intensity"],
                        color="precipitation_intensity",
                        color_continuous_scale="temps",
                        title="Tunnel Precipitation Map",
                        zoom=10,
                        range_color=[0, 10]) 

                    
# Set the map style
fig.update_layout(mapbox_style="open-street-map")


# # Streamlit app layout
# st.title("Eindhoven Weather Dashboard")

# # Display today's weather
# st.subheader(f"Today's Weather ({current_day_forecast['date']})")
# st.metric("Average Temperature", f"{avg_temp_today:.2f}°C")
# st.metric("Average Feels Like Temperature", f"{avg_feels_like_today:.2f}°C")
# st.metric("Total Rainfall (Today)", f"{total_rainfall_today:.2f} mm")
# st.metric("Peak Rainfall Time (Today)", f"{peak_rainfall_time_today}")
# st.info(f"Clothing Suggestion: {suggestion}")

# # Charts
# st.plotly_chart(temp_chart, use_container_width=True)
# st.plotly_chart(hum_chart, use_container_width=True)
# st.plotly_chart(rain_chart, use_container_width=True)
# st.plotly_chart(forecast_rain_chart, use_container_width=True)
# st.plotly_chart(historical_chart, use_container_width=True)

# # Display tomorrow's forecast
# st.subheader(f"Tomorrow's Weather Forecast ({next_day_forecast['date']})")
# st.dataframe({
#     "Time": times_tomorrow,
#     "Temperature (°C)": temperatures_tomorrow,
#     "Feels Like (°C)": feels_like_tomorrow,
#     "Precipitation (mm)": precipitation,
#     "Humidity (%)": humidity,
#     "Wind Speed (km/h)": wind_speed
# })

st.title("Eindhoven Weather Dashboard")

# Display today's weather in a grid
st.subheader(f"Today's Weather ({current_day_forecast['date']})")
col1, col2, col3 = st.columns(3)  # Create a three-column layout

with col1:
    st.metric("Average Temperature", f"{avg_temp_today:.2f}°C")
    st.metric("Peak Rainfall Time (Today)", f"{peak_rainfall_time_today}")

with col2:
    st.metric("Average Feels Like Temperature", f"{avg_feels_like_today:.2f}°C")
    st.metric("Total Rainfall (Today)", f"{total_rainfall_today:.2f} mm")

with col3:
    st.info(f"Clothing Suggestion: {suggestion}")

    # Display official weather warnings with Meteo
    country = "Netherlands"
    city = "Eindhoven"

    meteo = Meteoalert(country, city)

    # Get the weather alert
    alert = meteo.get_alert()
    if alert:
        # Display the alert with a red background
        st.markdown(
            f"""
            <div style="background-color: red; color: white; padding: 10px; border-radius: 5px;">
                <strong>ALERT:</strong> {alert}
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        # Display a "no alerts" message with a green background
        st.markdown(
            f"""
            <div style="background-color: green; color: white; padding: 10px; border-radius: 5px;">
                No current official alerts for Eindhoven (based on MeteoAlarm).
            </div>
            """,
            unsafe_allow_html=True
        )


# Display the charts in a grid
st.subheader("Weather Trends")

# Create a two-column layout for the charts
col4, col5 = st.columns(2)

with col4:
    st.plotly_chart(temp_chart, use_container_width=True)
    st.plotly_chart(rain_chart, use_container_width=True)


# Display the map

# Display tomorrow's forecast in a grid
st.subheader(f"Tomorrow's Weather Forecast ({next_day_forecast['date']})")
st.dataframe({
    "Time": times_tomorrow,
    "Temperature (°C)": temperatures_tomorrow,
    "Feels Like (°C)": feels_like_tomorrow,
    "Precipitation (mm)": precipitation,
    "Humidity (%)": humidity,
    "Wind Speed (km/h)": wind_speed
}, use_container_width=True)

with col5:
    st.plotly_chart(hum_chart, use_container_width=True)
    st.plotly_chart(forecast_rain_chart, use_container_width=True)

    ##historical_chart all alone in one grid



# Display the forecasted rainfall chart below
st.plotly_chart(historical_chart, use_container_width=True)
# Streamlit layout
st.title("Tunnel Precipitation Map")
st.plotly_chart(fig)

