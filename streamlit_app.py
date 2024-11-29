from dotenv import load_dotenv
import os
import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import datetime, timedelta

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

# Display the charts in a grid
st.subheader("Weather Trends")

# Create a two-column layout for the charts
col4, col5 = st.columns(2)

with col4:
    st.plotly_chart(temp_chart, use_container_width=True)
    st.plotly_chart(rain_chart, use_container_width=True)

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

