
import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date

class WeatherDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data):
        timestamp, temperature, wind_speed, _, \
        humidity, pressure, visibility, _ = data

        formatted_timestamp = format_date(timestamp)

        return {
            "temperature": temperature,
            "wind_speed": wind_speed,
            "humidity": humidity,
            "pressure": pressure,
            "visibility": visibility,
            "timestamp": formatted_timestamp
        }