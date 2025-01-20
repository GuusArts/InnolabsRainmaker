from supabase import create_client
import os
import streamlit as st
from dotenv import load_dotenv
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import date

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Fetch data from Supabase
def fetch_table_data(table_name):
    response = supabase.table(table_name).select("*").execute()
    if hasattr(response, 'error'):
        st.error(f"Error fetching data from {table_name}: {response.error.message}")
        return None
    return pd.DataFrame(response.data)

# Fetch all required data
weather_data = fetch_table_data("weather_data_baltic")
forecast_weather = fetch_table_data("forecast_weather_baltic")
today_weather_trends = fetch_table_data("today_weather_trends_baltic")
historical_precipitation = fetch_table_data("precipitation_trends_baltic")
tomorrow_weather = fetch_table_data("tomorrow_weather_baltic")

# Display Dashboard
st.title("Baltic Weather Dashboard")

# Today's Weather
if not weather_data.empty:
    # Filter for today's data
    today_date = str(date.today())
    today_data = weather_data[weather_data["date"] == today_date]

    if not today_data.empty:
        # Sort by 'created_at' and pick the latest record
        today_data = today_data.sort_values(by="created_at", ascending=False).iloc[0]

        st.subheader(f"Today's Weather ({today_data['date']})")
        col1, col2, col3 = st.columns(3)  # Three columns layout
        with col1:
            st.metric("Average Temperature", f"{today_data['avg_temp']:.2f}°C")
            st.metric("Peak Rainfall Time", today_data['peak_rainfall_time'])
        with col2:
            st.metric("Average Feels Like Temperature", f"{today_data['avg_feels_like']:.2f}°C")
            st.metric("Total Rainfall (Today)", f"{today_data['total_rainfall']:.2f} mm")
        with col3:
            # Clothing Suggestion
            st.info(f"Clothing Suggestion: {today_data['suggestion']}")

            # Add space below the suggestion
            st.markdown("<br>", unsafe_allow_html=True)

            # Display Weather Alert Box below suggestion
            weather_alert = today_data.get("weather_alert", "No alerts")
            if weather_alert and weather_alert != "No alerts":
                st.markdown(
                    f"""
                    <div style="background-color: red; color: white; padding: 10px; border-radius: 5px; text-align: center; font-weight: bold;">
                        ALERT: {weather_alert} <br>(based on MeteoAlarm)
                    </div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"""
                    <div style="background-color: green; color: white; padding: 10px; border-radius: 5px; text-align: center; font-weight: bold;">
                        No current official alerts for the Baltic. <br>(based on MeteoAlarm)
                    </div>
                    """,
                    unsafe_allow_html=True
                )
    else:
        st.warning("No weather data available for today!")
    

# Weather Trends - Temperature and Humidity Trends Side-by-Side
if not today_weather_trends.empty:
    st.subheader("Weather Trends")

    # Convert 'time' column to datetime
    today_weather_trends["time"] = pd.to_datetime(today_weather_trends["time"])

    # Filter data for today's date
    today_date = date.today()
    today_trend = today_weather_trends[today_weather_trends["time"].dt.date == today_date]

    if not today_trend.empty:
        # Keep only the latest record for each unique time
        today_trend = today_trend.sort_values(by="time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)

        # Remove rows with NaN values in relevant columns
        today_trend = today_trend.dropna(subset=["temperature", "feels_like", "humidity"])

        # Extract data
        times_today = today_trend["time"].dt.strftime('%H:%M')  # Format time as 'HH:MM'
        temperatures_today = today_trend["temperature"].reset_index(drop=True)
        feels_like_today = today_trend["feels_like"].reset_index(drop=True)
        humidity_today = today_trend["humidity"].reset_index(drop=True)

        # Identify min and max values for Temperature
        min_temp_today = temperatures_today.min()
        max_temp_today = temperatures_today.max()
        min_temp_time_today = times_today.iloc[temperatures_today.idxmin()]
        max_temp_time_today = times_today.iloc[temperatures_today.idxmax()]

        # Identify min and max values for Humidity
        min_humidity_today = humidity_today.min()
        max_humidity_today = humidity_today.max()
        min_humidity_time_today = times_today.iloc[humidity_today.idxmin()]
        max_humidity_time_today = times_today.iloc[humidity_today.idxmax()]

        # Create Temperature Chart
        temp_chart = go.Figure()
        temp_chart.add_trace(go.Scatter(x=times_today, y=temperatures_today, 
                                        mode='lines+markers', name='Temperature (°C)', line=dict(color='blue')))
        temp_chart.add_trace(go.Scatter(x=times_today, y=feels_like_today, 
                                        mode='lines+markers', name='Feels Like (°C)', line=dict(color='orange')))
        temp_chart.add_trace(go.Scatter(
            x=[min_temp_time_today], y=[min_temp_today], mode='markers',
            marker=dict(color='red', size=10), showlegend=False))
        temp_chart.add_trace(go.Scatter(
            x=[max_temp_time_today], y=[max_temp_today], mode='markers',
            marker=dict(color='green', size=10), showlegend=False))
        temp_chart.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers', marker=dict(color='red', size=10), name='Lowest Temp'))
        temp_chart.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers', marker=dict(color='green', size=10), name='Highest Temp'))
        temp_chart.update_layout(
            title="Temperature Trend Throughout the Day (Today)",
            xaxis_title="Time", yaxis_title="Temperature (°C)",
            legend_title="Legend", template="plotly_white"
        )

        # Create Humidity Chart
        hum_chart = go.Figure()
        hum_chart.add_trace(go.Scatter(x=times_today, y=humidity_today, 
                                       mode='lines+markers', name='Humidity (%)', line=dict(color='blue')))
        hum_chart.add_trace(go.Scatter(
            x=[min_humidity_time_today], y=[min_humidity_today], mode='markers',
            marker=dict(color='red', size=10), showlegend=False))
        hum_chart.add_trace(go.Scatter(
            x=[max_humidity_time_today], y=[max_humidity_today], mode='markers',
            marker=dict(color='green', size=10), showlegend=False))
        hum_chart.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers', marker=dict(color='red', size=10), name='Lowest Humidity'))
        hum_chart.add_trace(go.Scatter(
            x=[None], y=[None], mode='markers', marker=dict(color='green', size=10), name='Highest Humidity'))
        hum_chart.update_layout(
            title="Humidity Trend Throughout the Day (Today)",
            xaxis_title="Time", yaxis_title="Humidity (%)",
            legend_title="Legend", template="plotly_white"
        )

        # Display charts side-by-side
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(temp_chart, use_container_width=True)
        with col2:
            st.plotly_chart(hum_chart, use_container_width=True)
    else:
        st.warning("No valid weather trend data available for today!")
else:
    st.warning("No weather trend data available!")

# Create two columns for displaying charts side by side
col1, col2 = st.columns(2)

# Column 1: Rainfall Trend for Today
with col1:
    st.subheader("Rainfall Trend Throughout the Day (Today)")

    # Filter for today's date
    today_date = date.today()
    today_trend = today_weather_trends[
        pd.to_datetime(today_weather_trends["time"]).dt.date == today_date
    ]

    if not today_trend.empty:
        # Extract times and rainfall data
        times_today = pd.to_datetime(today_trend["time"]).dt.strftime('%H:%M')
        rainfall_today = today_trend["rainfall"]

        # Create Rainfall Trend Chart for Today
        rain_today_chart = go.Figure()
        rain_today_chart.add_trace(go.Scatter(
            x=times_today, y=rainfall_today, mode='lines', fill='tozeroy',
            name='Rainfall (mm)', line=dict(color='blue')
        ))

        # Add horizontal lines for rainfall levels
        rain_today_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
        rain_today_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
        rain_today_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")

        # Update layout
        rain_today_chart.update_layout(
            xaxis_title="Time", yaxis_title="Rainfall (mm)",
            template="plotly_white", xaxis=dict(tickangle=45)
        )

        # Display the chart
        st.plotly_chart(rain_today_chart, use_container_width=True)
    else:
        st.warning("No rainfall data available for today!")

# Column 2: Forecasted Rainfall Trends
with col2:
    st.subheader("Forecasted Rainfall Trends for Upcoming Days")

    # Convert 'time' column to datetime
    forecast_weather["time"] = pd.to_datetime(forecast_weather["time"])

    # Filter for upcoming dates (including today)
    upcoming_forecast = forecast_weather[forecast_weather["time"].dt.date >= today_date]

    if not upcoming_forecast.empty:
        # Aggregate rainfall by date
        upcoming_forecast["date"] = upcoming_forecast["time"].dt.date
        forecasted_rainfall = upcoming_forecast.groupby("date")["precipitation"].sum().reset_index()

        # Extract forecasted dates and rainfall
        forecasted_dates = forecasted_rainfall["date"]
        forecasted_rainfall_values = forecasted_rainfall["precipitation"]

        # Create Forecasted Rainfall Chart
        forecast_rain_chart = go.Figure()
        forecast_rain_chart.add_trace(go.Scatter(
            x=forecasted_dates, y=forecasted_rainfall_values,
            mode='lines+markers', fill='tozeroy',
            name='Forecasted Rainfall (mm)', line=dict(color='blue')
        ))

        # Add horizontal lines for rainfall levels
        forecast_rain_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
        forecast_rain_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
        forecast_rain_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")

        # Update layout
        forecast_rain_chart.update_layout(
            xaxis_title="Date", yaxis_title="Rainfall (mm)",
            template="plotly_white", xaxis=dict(tickangle=45)
        )

        # Display the chart
        st.plotly_chart(forecast_rain_chart, use_container_width=True)
    else:
        st.warning("No forecasted weather data available for upcoming days!")

# Detailed Tomorrow's Weather Forecast in Grid Format
if "forecast_weather" in locals() and not forecast_weather.empty:
    st.subheader("Detailed Tomorrow's Weather Forecast")

    # Step 1: Calculate tomorrow's date
    tomorrow_date = pd.Timestamp.now().date() + pd.Timedelta(days=1)

    # Step 2: Ensure 'time' column is datetime and filter for tomorrow
    forecast_weather["time"] = pd.to_datetime(forecast_weather["time"], errors="coerce")
    tomorrow_data = forecast_weather[forecast_weather["time"].dt.date == tomorrow_date]

    if not tomorrow_data.empty:
        # Step 3: Sort data by time
        tomorrow_data = tomorrow_data.sort_values(by="time")

        # Step 4: Prepare data for display
        display_data = pd.DataFrame({
            "Time": tomorrow_data["time"].dt.strftime('%H:%M'),
            "Temperature (°C)": tomorrow_data["temperature"],
            "Feels Like (°C)": tomorrow_data["feels_like"],
            "Precipitation (mm)": tomorrow_data["precipitation"],
            "Humidity (%)": tomorrow_data["humidity"],
            "Wind Speed (km/h)": tomorrow_data["wind_speed"]
        })

        # Step 5: Display in a grid format
        st.dataframe(display_data, use_container_width=True)

    else:
        st.warning("No forecast data available for tomorrow!")
else:
    st.warning("Forecast weather table is empty or unavailable!")

# Historical Precipitation Chart with Continuous Date Range
if "historical_precipitation" in locals() and not historical_precipitation.empty:
    st.subheader("Precipitation Trends Over the Last 7 Days")

    try:
        # Step 1: Ensure 'date' column is a datetime column
        historical_precipitation["date"] = pd.to_datetime(historical_precipitation["date"], errors="coerce")

        # Step 2: Calculate today and determine the last valid date in the table
        today = pd.Timestamp.now().date()
        max_date = historical_precipitation["date"].max().date()

        # Step 3: Dynamically calculate the range (up to 7 complete days)
        end_date = min(max_date, today - pd.Timedelta(days=1))  # Use the most recent complete day
        start_date = max(end_date - pd.Timedelta(days=6), historical_precipitation["date"].min().date())  # Adjust for available data

        # Step 4: Generate full date range and merge with existing data
        date_range = pd.date_range(start=start_date, end=end_date)
        full_data = pd.DataFrame({"date": date_range})
        merged_data = full_data.merge(
            historical_precipitation[["date", "precipitation"]],
            on="date", how="left"
        ).fillna({"precipitation": 0})  # Fill missing precipitation values with 0

        # Step 5: Extract data for the chart
        dates = merged_data["date"].dt.strftime('%Y-%m-%d')
        daily_precipitation = merged_data["precipitation"]

        # Step 6: Create the chart
        historical_chart = go.Figure()
        historical_chart.add_trace(go.Scatter(
            x=dates, y=daily_precipitation,
            mode='lines+markers', fill='tozeroy',
            name='Daily Precipitation (mm)'
        ))
        # Add horizontal lines for rainfall levels
        historical_chart.add_hline(y=1, line_dash="dash", annotation_text="Light Rain", line_color="blue")
        historical_chart.add_hline(y=5, line_dash="dash", annotation_text="Moderate Rain", line_color="orange")
        historical_chart.add_hline(y=10, line_dash="dash", annotation_text="Heavy Rain", line_color="red")

        # Step 7: Update chart layout
        historical_chart.update_layout(
            title=f"Precipitation Trends ({start_date} to {end_date})",
            xaxis_title="Date",
            yaxis_title="Precipitation (mm)",
            template="plotly_white",
            xaxis=dict(type='category')  # Ensure dates appear cleanly on x-axis
        )

        # Step 8: Display the chart
        st.plotly_chart(historical_chart, use_container_width=True)
    except Exception as e:
        st.error(f"Error processing precipitation trends data: {e}")
else:
    st.warning("No data available in the 'precipitation_trends' table!")

