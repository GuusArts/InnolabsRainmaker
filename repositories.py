from dagster import repository
from database_data_pipeline import today_weather_trends_pipeline, forecast_weather_pipeline, tomorrow_weather_pipeline, historical_precipitation_pipeline, tunnel_pipeline
from riga_pipeline import riga_today_weather_trends_pipeline, riga_forecast_weather_pipeline, riga_tomorrow_weather_pipeline,riga_historical_precipitation_pipeline
from email_pipeline import email_pipeline

@repository
def combined_pipeline_repository():
    return [
        # Add pipelines from database_data_pipeline
        today_weather_trends_pipeline,
        forecast_weather_pipeline,
        tomorrow_weather_pipeline,
        historical_precipitation_pipeline,
        tunnel_pipeline,

        # Add pipelines from riga_pipeline
        riga_today_weather_trends_pipeline,
        riga_forecast_weather_pipeline,
        riga_tomorrow_weather_pipeline,
        riga_historical_precipitation_pipeline,

        # Add pipelines from email_pipeline
        email_pipeline,
    ]
