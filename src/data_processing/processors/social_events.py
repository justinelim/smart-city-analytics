
import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date
from datetime import datetime

class SocialEventsDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data):
        # if not data or len(data) < 5:
        #     # Handle the case where data is None or does not have enough elements
        #     logging.warning(f"Invalid data: {data}")
        #     return None
        
        event_type, _, _, _, \
        event_date = data

        formatted_event_date = format_date(event_date)

        return {
            "event_type": event_type,
            "event_date": formatted_event_date
        }