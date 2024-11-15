import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import datetime

# Weather API setup
api_key = "be605a1125674db58dc221827241311"  # Replace with your API key
location = "Eindhoven"

# Fetch weather data
url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
response = requests.get(url)
data = response.json()

# Check if 'forecast' exists in the response
if 'forecast' not in data or 'forecastday' not in data['forecast']:
    st.error("Invalid response: 'forecast' data is missing")
    st.stop()

# Extract current day's forecast
current_day_forecast = data['forecast']['forecastday'][0]
hourly_data_today = current_day_forecast['hour']
temperatures_today = [hour['temp_c'] for hour in hourly_data_today]
feels_like_today = [hour['feelslike_c'] for hour in hourly_data_today]
times_today = [datetime.strptime(hour['time'], '%Y-%m-%d %H:%M').strftime('%H:%M') for hour in hourly_data_today]

# Extract next day's forecast
next_day_forecast = data['forecast']['forecastday'][1]
hourly_data_tomorrow = next_day_forecast['hour']
temperatures_tomorrow = [hour['temp_c'] for hour in hourly_data_tomorrow]
feels_like_tomorrow = [hour['feelslike_c'] for hour in hourly_data_tomorrow]
precipitation = [hour['precip_mm'] for hour in hourly_data_tomorrow]
humidity = [hour['humidity'] for hour in hourly_data_tomorrow]
wind_speed = [hour['wind_kph'] for hour in hourly_data_tomorrow]
times_tomorrow = [datetime.strptime(hour['time'], '%Y-%m-%d %H:%M').strftime('%H:%M') for hour in hourly_data_tomorrow]

# Calculate averages for current day
avg_temp_today = sum(temperatures_today) / len(temperatures_today)
avg_feels_like_today = sum(feels_like_today) / len(feels_like_today)

# Find lowest and highest temperatures for current day
min_temp_today = min(temperatures_today)
max_temp_today = max(temperatures_today)
min_temp_time_today = times_today[temperatures_today.index(min_temp_today)]
max_temp_time_today = times_today[temperatures_today.index(max_temp_today)]

# Clothing recommendations based on average temperature
if avg_temp_today < 7:
    suggestion = "Take your gloves!"
elif avg_temp_today < 13:
    suggestion = "Bring your hat!"
else:
    suggestion = "No special clothing needed today."

# Create interactive line chart for the current day's forecast
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

# Display the Streamlit app
st.title("Eindhoven Weather Dashboard")
st.subheader(f"Date: {current_day_forecast['date']}")
st.metric("Average Temperature (Today)", f"{avg_temp_today:.2f}°C")
st.metric("Average Feels Like Temperature (Today)", f"{avg_feels_like_today:.2f}°C")
st.info(f"Clothing Suggestion: {suggestion}")

st.plotly_chart(temp_chart, use_container_width=True)

st.subheader(f"Tomorrow's Weather Forecast ({next_day_forecast['date']})")
st.dataframe({
    "Time": times_tomorrow,
    "Temperature (°C)": temperatures_tomorrow,
    "Feels Like (°C)": feels_like_tomorrow,
    "Precipitation (mm)": precipitation,
    "Humidity (%)": humidity,
    "Wind Speed (km/h)": wind_speed
})

