from dagster import op, job, In, repository, DagsterTypeCheckError
import requests
import json
from dotenv import load_dotenv
import os

def configure():
    """Load API key from .env file."""
    load_dotenv()

configure()
api_key = os.getenv('api_key')

# Define operations (ops)
@op
def fetch_weather_data(context) -> dict:
    """Fetch weather data."""
    location = "Eindhoven"
    url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={location}&days=2"
    response = requests.get(url)
    context.log.info(f"Fetched weather data for {location}")
    return response.json()

@op
def fetch_tunnel_data(context) -> list:
    """Fetch tunnel data."""
    url = "https://data.eindhoven.nl/api/explore/v2.1/catalog/datasets/tunnelvisie-punten/records?limit=71"
    response = requests.get(url)
    context.log.info("Fetched tunnel data")
    return response.json().get('results', [])

@op
def weather_data_type() -> str:
    """Provide the data type for weather."""
    return "weather"

@op
def tunnel_data_type() -> str:
    """Provide the data type for tunnel."""
    return "tunnel"

@op
def generate_file_name(context, data_type: str) -> str:
    """Generate a file name based on data type."""
    file_name = f"{data_type}_data.json"
    context.log.info(f"Generated file name: {file_name}")
    return file_name

@op(ins={"data": In(), "file_name": In(str)})
def store_data(context, data, file_name: str):
    """Store data to a file."""
    if not isinstance(data, (list, dict)):
        raise DagsterTypeCheckError(f"store_data expects a list or dict, but got {type(data)}.")
    with open(file_name, "w") as f:
        json.dump(data, f)
    context.log.info(f"Data stored in {file_name}")

# Define the job

@job
def fetch_and_store_data():
    """Job to fetch and store weather and tunnel data."""
    weather_data = fetch_weather_data()
    weather_data_type_output = weather_data_type()
    weather_file_name = generate_file_name(weather_data_type_output)
    store_data(weather_data, weather_file_name)

    tunnel_data = fetch_tunnel_data()
    tunnel_data_type_output = tunnel_data_type()
    tunnel_file_name = generate_file_name(tunnel_data_type_output)
    store_data(tunnel_data, tunnel_file_name)

# Repository definition for Dagster

@repository
def my_repo():
    return [fetch_and_store_data]