from dotenv import load_dotenv
import os
from supabase import create_client
from dagster import op, job
import yagmail
import pandas as pd
from datetime import date


# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_table_data(table_name):
    """Fetches data from the specified Supabase table."""
    response = supabase.table(table_name).select("*").execute()
    if hasattr(response, 'error'):
        raise Exception(f"Error fetching data from {table_name}: {response.error.message}")
    return pd.DataFrame(response.data)

def generate_today_weather_summary(weather_data):
    """Generates a summary of today's weather."""
    today_date = str(date.today())
    today_data = weather_data[weather_data["date"] == today_date]

    if not today_data.empty:
        today_data = today_data.sort_values(by="created_at", ascending=False).iloc[0]
        return f"""
        Today's Weather:
        - Average Temperature: {today_data['avg_temp']:.2f}°C
        - Average Feels Like Temperature: {today_data['avg_feels_like']:.2f}°C
        - Peak Rainfall Time: {today_data['peak_rainfall_time']}
        - Total Rainfall: {today_data['total_rainfall']:.2f} mm
        - Official Alert: {today_data.get('weather_alert', 'No alerts')}
        """
    return "No weather data available for today."

def generate_weather_trends_summary(today_weather_trends):
    """Generates a summary of today's weather trends."""
    today_weather_trends["time"] = pd.to_datetime(today_weather_trends["time"])
    today_date = date.today()
    today_trend = today_weather_trends[today_weather_trends["time"].dt.date == today_date]

    if not today_trend.empty:
        today_trend = today_trend.sort_values(by="time").drop_duplicates(subset=["time"], keep="last")
        temperatures = today_trend["temperature"]
        humidity = today_trend["humidity"]

        return f"""
        Weather Trends:
        - Highest Temperature: {temperatures.max():.2f}°C
        - Lowest Temperature: {temperatures.min():.2f}°C
        - Highest Humidity: {humidity.max():.2f}%
        - Lowest Humidity: {humidity.min():.2f}%
        """
    return "No weather trend data available for today."

def generate_forecasted_rainfall_summary(forecast_weather):
    """Generates a summary of upcoming forecasted rainfall."""
    forecast_weather["time"] = pd.to_datetime(forecast_weather["time"])
    today_date = date.today()
    upcoming_forecast = forecast_weather[forecast_weather["time"].dt.date >= today_date]

    if not upcoming_forecast.empty:
        upcoming_forecast["date"] = upcoming_forecast["time"].dt.date
        forecasted_rainfall = upcoming_forecast.groupby("date")["precipitation"].sum().reset_index()

        return "\n".join(
            [f"- {row['date']}: {row['precipitation']:.2f} mm" for _, row in forecasted_rainfall.iterrows()]
        )
    return "No forecasted weather data available."

def generate_tunnel_precipitation_summary(tunnel_data):
    """Generates a summary of tunnels with highest precipitation."""
    if not tunnel_data.empty:
        tunnel_data = tunnel_data[tunnel_data["precipitation_intensity"] > 0]
        top_tunnels = tunnel_data.nlargest(5, "precipitation_intensity")

        return "\n".join(
            [
                f"- {row['location_name']}: {row['precipitation_intensity']:.2f} mm"
                for _, row in top_tunnels.iterrows()
            ]
        )
    return "No data available for tunnel precipitation."

# Email Operation
@op
def send_email_with_yagmail():
    """Fetches data, generates summaries, and sends an email using Yagmail."""
    sender_email = os.getenv("sender_email")
    app_password = os.getenv("app_password")
    receiver_email = os.getenv("receiver_email")

    # Fetch necessary data
    weather_data = fetch_table_data("weather_data")
    forecast_weather = fetch_table_data("forecast_weather")
    today_weather_trends = fetch_table_data("today_weather_trends")
    tunnel_data = fetch_table_data("tunnel_data")

    # Generate summaries
    summary = f"""
    Eindhoven Daily Summary:

    {generate_today_weather_summary(weather_data)}

    {generate_weather_trends_summary(today_weather_trends)}

    Forecasted Rainfall Trends:
    {generate_forecasted_rainfall_summary(forecast_weather)}

    Tunnel Precipitation (Top 5):
    {generate_tunnel_precipitation_summary(tunnel_data)}
    """

    subject = "Eindhoven Daily Summary"

    try:
        # Initialize Yagmail SMTP client
        yag = yagmail.SMTP(user=sender_email, password=app_password)
        yag.send(to=receiver_email, subject=subject, contents=summary)
        print("Email sent successfully!")
    except Exception as e:
        raise Exception(f"Failed to send email: {e}")

# Job
@job
def email_pipeline():
    """Pipeline to send emails."""
    send_email_with_yagmail()

# @schedule(
#     cron_schedule="5 15 * * *",  # Runs at 15:05
#     job=email_pipeline,
#     execution_timezone="Europe/Amsterdam"
# )
# def email_summary_schedule(_context):
#     """Schedule to trigger the email_pipeline."""
#     return {}
