
import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date

class PollutionDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, data):
        ozone, particulate_matter, carbon_monoxide, sulfur_dioxide, \
        nitrogen_dioxide, _, _, timestamp = data

        formatted_timestamp = format_date(timestamp)

        return {
            "ozone": ozone,
            "particulate_matter": particulate_matter,
            "carbon_monoxide": carbon_monoxide,
            "sulfur_dioxide": sulfur_dioxide,
            "nitrogen_dioxide": nitrogen_dioxide,
            "timestamp": formatted_timestamp
        }