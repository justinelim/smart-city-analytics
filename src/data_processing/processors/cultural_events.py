
import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date

NO_OF_FIELDS = 19

class CulturalEventsDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data):
        if len(data) != 19:
            self.logger.error(f"Unexpected data length: {len(data)}. Data: {data}")
            return None
        _, _, _, _, avg_ticket_price, _, _, _, event_id, _, _, _, event_date, _, _, _, _, _, event_type = data
        formatted_event_date = format_date(event_date)
        
        return {
            "event_id": str(event_id), # INT
            "event_date": formatted_event_date, # DATETIME
            "event_type": event_type, # STR
            "avg_ticket_price": str(avg_ticket_price) # FLOAT
        }