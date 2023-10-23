
import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date
from datetime import datetime

def calculate_duration(start_time, end_time):
    start_datetime = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    end_datetime = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    duration = end_datetime - start_datetime
    return duration.total_seconds() / 60

class LibraryEventsDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, data):
        # if len(data) != 19:
        #     logging.error(f"Unexpected data length: {len(data)}. Data: {data}")
        #     return None
        _, city, end_time, _, \
        _, _, _, _, \
        _, library, _, _, \
        _, _, _, start_time, \
        _, _, _, _ = data

        formatted_start_time = format_date(start_time)
        formatted_end_time = format_date(end_time)
        duration = round(calculate_duration(formatted_start_time, formatted_end_time), 2)

        return {
            "city": city,
            "event_start_time": formatted_start_time,
            "library": library,
            "duration": duration
        }