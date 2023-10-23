import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date

class RoadTrafficDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic):
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)

    def transform_data(self, data):
        self.logger.debug('Processing road traffic data...')
        super().transform_data(data)

        _, avg_measured_time, _, _, \
        _, timestamp, vehicle_count, _, \
        _ = data

        formatted_timestamp = format_date(timestamp)

        return {
            "avg_measured_time": avg_measured_time,
            "timestamp": formatted_timestamp,
            "vehicle_count": vehicle_count
        }