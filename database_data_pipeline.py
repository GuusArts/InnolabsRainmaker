from dagster import job, op, repository
import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime
import pytz

# Load environment variables
load_dotenv()
api_key = os.getenv('api_key')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

location = "Eindhoven"

# ------------------
# Operations
# ------------------

@op
def fetch_weather_data():
    """Fetches weather data from the weather API."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@op
def process_weather_data(weather_data):
    """Processes weather data to extract meaningful information."""
    forecast = weather_data.get("forecast", {}).get("forecastday", [])
    if not forecast:
        raise ValueError("No forecast data available")

    today = forecast[0]
    avg_temp = sum(hour["temp_c"] for hour in today["hour"]) / len(today["hour"])
    avg_feels_like = sum(hour["feelslike_c"] for hour in today["hour"]) / len(today["hour"])
    total_rainfall = sum(hour["precip_mm"] for hour in today["hour"])

    peak_rainfall_hour = max(today["hour"], key=lambda h: h["precip_mm"])
    peak_rainfall_time = peak_rainfall_hour["time"]

    # Generate clothing suggestion
    if total_rainfall > 0.5:
        suggestion = "Bring an umbrella!" if avg_temp > 13 else "Take your gloves and umbrella!"
    else:
        suggestion = "No special clothing needed today."

    return {
        "date": today["date"],
        "location": location,
        "avg_temp": avg_temp,
        "avg_feels_like": avg_feels_like,
        "total_rainfall": total_rainfall,
        "peak_rainfall_time": peak_rainfall_time,
        "suggestion": suggestion,
    }

@op
def store_weather_data(weather_data):
    """Inserts processed weather data into Supabase."""
    local_tz = pytz.timezone("Europe/Amsterdam")  # Adjust for your timezone
    weather_data["created_at"] = datetime.now(local_tz).isoformat()  # Add local timestamp
    supabase.table("weather_data").insert(weather_data).execute()

@op
def fetch_tunnel_data():
    """Fetches tunnel data from the Eindhoven API."""
    url = "https://data.eindhoven.nl/api/explore/v2.1/catalog/datasets/tunnelvisie-punten/records?limit=71"
    response = requests.get(url)
    response.raise_for_status()
    tunnels = response.json().get("results", [])
    return tunnels

@op
def process_tunnel_data(tunnels):
    """Processes tunnel data by adding precipitation information."""
    processed_tunnels = []
    for tunnel in tunnels:
        lat = float(tunnel["lat"])
        lon = float(tunnel["lon"])
        location_name = tunnel["locatienaam"]
        
        # Clean the year field
        raw_year = tunnel.get("jaar", None)
        try:
            # Extract the first valid year if a range is provided (e.g., "2017/2022")
            year = int(raw_year.split("/")[0]) if isinstance(raw_year, str) and "/" in raw_year else int(raw_year)
        except (ValueError, TypeError):
            year = None  # Set to None if parsing fails

        # Fetch precipitation data for each tunnel
        precip_response = requests.get(f"https://gps.buienradar.nl/getrr.php?lat={lat}&lon={lon}")
        precip_data = precip_response.text.strip()
        precipitation_description = "No rain"
        precipitation_intensity = 0

        # Process precipitation data
        for line in precip_data.splitlines():
            parts = line.split("|")
            if len(parts) == 2:
                try:
                    intensity = 10 ** ((int(parts[0]) - 109) / 32)
                    precipitation_intensity = max(precipitation_intensity, intensity)
                except ValueError:
                    continue

        # Set description based on intensity
        if precipitation_intensity < 0.1:
            precipitation_description = "No rain"
        elif precipitation_intensity <= 2.5:
            precipitation_description = "Light rain"
        elif precipitation_intensity <= 7.5:
            precipitation_description = "Moderate rain"
        else:
            precipitation_description = "Heavy rain"

        processed_tunnels.append({
            "location_name": location_name,
            "year": year,  # Cleaned year value
            "latitude": lat,
            "longitude": lon,
            "precipitation_description": precipitation_description,
            "precipitation_intensity": precipitation_intensity,
        })

    return processed_tunnels


@op
def store_tunnel_data(processed_tunnels):
    """Inserts processed tunnel data into Supabase."""
    local_tz = pytz.timezone("Europe/Amsterdam")  # Adjust for your timezone
    for tunnel in processed_tunnels:
        tunnel["created_at"] = datetime.now(local_tz).isoformat()  # Add local timestamp
    supabase.table("tunnel_data").insert(processed_tunnels).execute()


@op
def fetch_historical_precipitation():
    """Fetches historical precipitation data."""
    dates = []
    precipitation = []
    for i in range(7):  # Past 7 days
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        history_url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date}"
        response = requests.get(history_url)
        response.raise_for_status()
        history_data = response.json()
        dates.append(date)
        precipitation.append(history_data["forecast"]["forecastday"][0]["day"]["totalprecip_mm"])

    return [{"date": date, "precipitation": precip, "type": "historical"} for date, precip in zip(dates, precipitation)]

@op
def store_precipitation_trends(trends):
    """Inserts precipitation trends into Supabase."""
    supabase.table("precipitation_trends").insert(trends).execute()

# ------------------
# Jobs
# ------------------

@job
def weather_pipeline():
    weather_data = fetch_weather_data()
    processed_weather = process_weather_data(weather_data)
    store_weather_data(processed_weather)

@job
def tunnel_pipeline():
    tunnels = fetch_tunnel_data()
    processed_tunnels = process_tunnel_data(tunnels)
    store_tunnel_data(processed_tunnels)

@job
def precipitation_pipeline():
    trends = fetch_historical_precipitation()
    store_precipitation_trends(trends)

# ------------------
# Repositories
# ------------------

@repository
def data_pipeline_repository():
    return [weather_pipeline, tunnel_pipeline, precipitation_pipeline]