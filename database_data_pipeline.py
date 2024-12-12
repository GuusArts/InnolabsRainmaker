from dagster import job, op, repository
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
import pytz
import logging


# Load environment variables
load_dotenv()
api_key = os.getenv('api_key')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

location = "Eindhoven"
local_tz = pytz.timezone("Europe/Amsterdam")  # Amsterdam timezone

# ------------------
# Operations
# ------------------

# Weather operations
@op
def fetch_weather_data():
    """Fetches weather data from the WeatherAPI."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@op
def process_weather_trends(weather_data):
    """Processes hourly trends for today's weather."""
    today = weather_data["forecast"]["forecastday"][0]["hour"]
    trends = [
        {
            "time": hour["time"],
            "temperature": hour["temp_c"],
            "feels_like": hour["feelslike_c"],
            "humidity": hour["humidity"],
            "rainfall": hour["precip_mm"],
            "created_at": datetime.now(local_tz).isoformat(),  # Add timestamp
        }
        for hour in today
    ]
    return trends

@op
def store_weather_data(weather_data):
    """Inserts processed weather data into Supabase."""
    try:
        local_tz = pytz.timezone("Europe/Amsterdam")
        today_data = weather_data["forecast"]["forecastday"][0]["day"]

        # Extract necessary fields
        avg_temp = today_data["avgtemp_c"]
        total_rainfall = today_data["totalprecip_mm"]
        avg_feels_like = sum(hour["feelslike_c"] for hour in weather_data["forecast"]["forecastday"][0]["hour"]) / len(weather_data["forecast"]["forecastday"][0]["hour"])
        peak_rainfall_time = max(
            weather_data["forecast"]["forecastday"][0]["hour"],
            key=lambda h: h["precip_mm"]
        )["time"]

        # Weather alert processing
        alert = weather_data.get("alerts", {}).get("alert", [])
        weather_alert = alert[0]["headline"] if alert else "No alerts"

        # Prepare data for insertion
        processed_data = {
            "date": weather_data["location"]["localtime"].split(" ")[0],
            "location": weather_data["location"]["name"],
            "avg_temp": avg_temp,
            "avg_feels_like": avg_feels_like,
            "total_rainfall": total_rainfall,
            "peak_rainfall_time": peak_rainfall_time,
            "suggestion": "Bring an umbrella!" if total_rainfall > 0.5 else "No special clothing needed.",
            "weather_alert": weather_alert,
            "created_at": datetime.now(local_tz).isoformat()
        }

        # Insert into Supabase
        response = supabase.table("weather_data").insert(processed_data).execute()

        # Check if an error occurred during insertion
        if response.error:
            raise ValueError(f"Failed to save weather data: {response.error.message}")
        else:
            logging.info("Successfully inserted weather data into Supabase")

    except Exception as e:
        logging.error(f"Error storing weather data: {e}")
        raise




@op
def store_today_weather_trends(trends):
    """Inserts hourly weather trends for today into Supabase."""
    supabase.table("today_weather_trends").insert(trends).execute()

@op
def process_forecast_data(weather_data):
    """Processes forecasted rainfall data for the upcoming two days."""
    forecast = [
        {
            "date": day["date"],
            "total_rainfall": day["day"]["totalprecip_mm"],
            "created_at": datetime.now(local_tz).isoformat(),  # Add timestamp
        }
        for day in weather_data["forecast"]["forecastday"]
    ]
    return forecast

@op
def store_forecast_weather(forecast):
    """Inserts forecasted rainfall trends into Supabase."""
    supabase.table("forecast_weather").insert(forecast).execute()

@op
def process_tomorrow_weather(weather_data):
    """Processes tomorrow's hourly weather forecast."""
    tomorrow = weather_data["forecast"]["forecastday"][1]["hour"]
    forecast = [
        {
            "time": hour["time"],
            "temperature": hour["temp_c"],
            "feels_like": hour["feelslike_c"],
            "precipitation": hour["precip_mm"],
            "humidity": hour["humidity"],
            "wind_speed": hour["wind_kph"],
            "created_at": datetime.now(local_tz).isoformat(),  # Add timestamp
        }
        for hour in tomorrow
    ]
    return forecast

@op
def store_tomorrow_weather(forecast):
    """Inserts tomorrow's hourly weather data into Supabase."""
    supabase.table("tomorrow_weather").insert(forecast).execute()

# Tunnel data operations
@op
def fetch_tunnel_data():
    """Fetches tunnel data from the Eindhoven API."""
    url = "https://data.eindhoven.nl/api/explore/v2.1/catalog/datasets/tunnelvisie-punten/records?limit=71"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("results", [])

@op
def process_tunnel_data(tunnels):
    """Processes tunnel data and adds precipitation information."""
    processed_tunnels = []
    for tunnel in tunnels:
        lat = float(tunnel["lat"])
        lon = float(tunnel["lon"])
        location_name = tunnel["locatienaam"]
        
        # Clean and parse the 'jaar' field
        raw_year = tunnel.get("jaar", None)
        try:
            if isinstance(raw_year, str) and "/" in raw_year:
                year = int(raw_year.split("/")[0])  # Take the first year in the range
            else:
                year = int(raw_year) if raw_year else None
        except (ValueError, TypeError):
            year = None  # Set to None if parsing fails

        # Fetch precipitation data
        precip_response = requests.get(f"https://gps.buienradar.nl/getrr.php?lat={lat}&lon={lon}")
        precip_data = precip_response.text.strip()
        precipitation_intensity = 0

        for line in precip_data.splitlines():
            parts = line.split("|")
            if len(parts) == 2:
                try:
                    intensity = 10 ** ((int(parts[0]) - 109) / 32)
                    precipitation_intensity = max(precipitation_intensity, intensity)
                except ValueError:
                    continue

        precipitation_description = (
            "No rain" if precipitation_intensity < 0.1 else
            "Light rain" if precipitation_intensity <= 2.5 else
            "Moderate rain" if precipitation_intensity <= 7.5 else
            "Heavy rain"
        )

        processed_tunnels.append({
            "location_name": location_name,
            "year": year,  # Cleaned year value
            "latitude": lat,
            "longitude": lon,
            "precipitation_description": precipitation_description,
            "precipitation_intensity": precipitation_intensity,
            "created_at": datetime.now(local_tz).isoformat(),  # Add Amsterdam timezone timestamp
        })

    return processed_tunnels


@op
def store_tunnel_data(processed_tunnels):
    """Inserts processed tunnel data into Supabase."""
    supabase.table("tunnel_data").insert(processed_tunnels).execute()

# ------------------
# Jobs
# ------------------

@job
def today_weather_trends_pipeline():
    """Pipeline to process and store today's weather trends."""
    weather_data = fetch_weather_data()
    trends = process_weather_trends(weather_data)
    store_weather_data(weather_data)
    store_today_weather_trends(trends)

@job
def forecast_weather_pipeline():
    """Pipeline to process and store forecasted rainfall trends."""
    weather_data = fetch_weather_data()
    forecast = process_forecast_data(weather_data)
    store_forecast_weather(forecast)

@job
def tomorrow_weather_pipeline():
    """Pipeline to process and store tomorrow's hourly weather forecast."""
    weather_data = fetch_weather_data()
    tomorrow_data = process_tomorrow_weather(weather_data)
    store_tomorrow_weather(tomorrow_data)

@job
def tunnel_pipeline():
    """Pipeline to process and store tunnel data."""
    tunnels = fetch_tunnel_data()
    processed_tunnels = process_tunnel_data(tunnels)
    store_tunnel_data(processed_tunnels)

# ------------------
# Repository
# ------------------

@repository
def data_pipeline_repository():
    return [
        today_weather_trends_pipeline,
        forecast_weather_pipeline,
        tomorrow_weather_pipeline,
        tunnel_pipeline,
    ]
