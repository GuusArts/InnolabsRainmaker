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

location = 'Riga'
local_tz = pytz.timezone("Europe/Amsterdam")  # Amsterdam timezone

# ------------------
# Operations
# ------------------

# Weather operations
@op(name="riga_fetch_weather_data")
def fetch_weather_data():
    """Fetches weather data from the WeatherAPI."""
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

@op(name="riga_store_weather_data")
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
        response = supabase.table("weather_data_baltic").insert(processed_data).execute()

        # Check if an error occurred during insertion
        if hasattr(response, 'error'): 
            raise ValueError(f"Failed to save weather data: {response.error.message}")
        else:
            logging.info("Successfully inserted weather data into Supabase")

    except Exception as e:
        logging.error(f"Error storing weather data: {e}")
        raise


# @op
# def process_weather_trends(weather_data):
#     """Processes hourly trends for today's weather."""
#     today = weather_data["forecast"]["forecastday"][0]["hour"]
#     trends = [
#         {
#             "time": hour["time"],
#             "temperature": hour["temp_c"],
#             "feels_like": hour["feelslike_c"],
#             "humidity": hour["humidity"],
#             "rainfall": hour["precip_mm"],
#             "created_at": datetime.now(local_tz).isoformat(),  # Add timestamp
#         }
#         for hour in today
#     ]
#     return trends


@op(name="riga_process_weather_trends")
def process_weather_trends(weather_data):
    """Processes hourly trends for today's weather."""
    today = weather_data["forecast"]["forecastday"][0]["hour"]

    # Debug log
    for hour in today:
        print(f"Hour data: {hour}")

    trends = [
        {
            "time": hour["time"],
            "temperature": hour["temp_c"],
            "feels_like": hour["feelslike_c"],
            "humidity": hour["humidity"],
            "rainfall": hour["precip_mm"],
            "created_at": datetime.now(local_tz).isoformat(),
        }
        for hour in today
    ]
    return trends


@op(name="riga_store_today_weather_trends")
def store_today_weather_trends(trends):
    """Inserts hourly weather trends for today into Supabase."""
    supabase.table("today_weather_trends_baltic").insert(trends).execute()

@op(name="riga_process_forecast_data")
def process_forecast_data(weather_data):
    """Processes hourly and daily forecasted data for upcoming days."""
    forecast = []

    # Iterate through forecast days
    for day in weather_data["forecast"]["forecastday"]:
        # Calculate total daily rainfall
        total_rainfall = day["day"]["totalprecip_mm"]

        for hour in day["hour"]:
            # Convert the hourly time to Amsterdam timezone
            time_utc = datetime.strptime(hour["time"], "%Y-%m-%d %H:%M")
            time_local = pytz.utc.localize(time_utc).astimezone(local_tz).strftime("%Y-%m-%d %H:%M:%S")

            forecast.append({
                "date": day["date"],
                "time": time_local,
                "temperature": hour["temp_c"],
                "feels_like": hour["feelslike_c"],
                "precipitation": hour["precip_mm"],
                "humidity": hour["humidity"],
                "wind_speed": hour["wind_kph"],
                "total_rainfall": total_rainfall,  # Include total daily rainfall
                "created_at": datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S"),
            })

    return forecast


@op(name="riga_store_forecast_weather")
def store_forecast_weather(forecast):
    """Inserts hourly forecasted data into Supabase."""
    try:
        # Insert forecast data into the forecast_weather table
        supabase.table("forecast_weather_baltic").insert(forecast).execute()
        logging.info("Successfully inserted forecast weather data into Supabase")
    except Exception as e:
        logging.error(f"Error inserting forecast weather data: {e}")
        raise

@op(name="riga_process_tomorrow_weather")
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

@op(name="riga_store_tomorrow_weather")
def store_tomorrow_weather(forecast):
    """Inserts tomorrow's hourly weather data into Supabase."""
    supabase.table("tomorrow_weather_baltic").insert(forecast).execute()

@op(name="riga_fetch_historical_precipitation")
def fetch_historical_precipitation():
    """Fetches historical precipitation data with Amsterdam timezone."""
    local_tz = pytz.timezone('Europe/Amsterdam')  # Define Amsterdam timezone
    dates = []
    precipitation = []
    
    for i in range(7):  # Past 7 days
        # Calculate the date in Amsterdam time
        date_amsterdam = (datetime.now(tz=local_tz) - timedelta(days=i)).strftime('%Y-%m-%d')
        history_url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date_amsterdam}"
        response = requests.get(history_url)
        response.raise_for_status()
        history_data = response.json()
        
        # Append data
        dates.append(date_amsterdam)
        precipitation.append(history_data["forecast"]["forecastday"][0]["day"]["totalprecip_mm"])
    
    # Add 'created_at' with Amsterdam timezone
    return [{
        "date": date,
        "precipitation": precip,
        "type": "historical",
        "created_at": datetime.now(local_tz).isoformat()  # Timestamp in Amsterdam timezone
    } for date, precip in zip(dates, precipitation)]

@op(name="riga_store_precipitation_trends")
def store_precipitation_trends(trends):
    """Inserts precipitation trends into Supabase."""
    supabase.table("precipitation_trends_baltic").insert(trends).execute()

# Tunnel data operations

# ------------------
# Jobs
# ------------------

@job
def riga_today_weather_trends_pipeline():
    """Pipeline to process and store today's weather trends."""
    weather_data = fetch_weather_data()
    trends = process_weather_trends(weather_data)
    store_weather_data(weather_data)
    store_today_weather_trends(trends)

@job
def riga_forecast_weather_pipeline():
    """Pipeline to process and store forecasted rainfall trends."""
    weather_data = fetch_weather_data()
    forecast = process_forecast_data(weather_data)
    store_forecast_weather(forecast)

@job
def riga_tomorrow_weather_pipeline():
    """Pipeline to process and store tomorrow's hourly weather forecast."""
    weather_data = fetch_weather_data()
    tomorrow_data = process_tomorrow_weather(weather_data)
    store_tomorrow_weather(tomorrow_data)

@job
def riga_historical_precipitation_pipeline():
    trends = fetch_historical_precipitation()
    store_precipitation_trends(trends)




# ------------------
# Repository
# ------------------

@repository
def riga_repository():
    return [
        riga_today_weather_trends_pipeline,
        riga_forecast_weather_pipeline,
        riga_tomorrow_weather_pipeline,
        riga_historical_precipitation_pipeline,
    ]
