import logging
from src.data_processing.data_processor import DataProcessor
from utils import format_date

class ParkingDataProcessor(DataProcessor):
    def __init__(self, config_path, kafka_topic) -> None:
        super().__init__(config_path, kafka_topic)
        self.logger = logging.getLogger(__name__)
        
    def transform_data(self, data):
        print('Inside transform_data, data is:', data)
        vehicle_count, update_time, _, total_spaces, garage_code, _, _, _, _, _, _, _ = data
        
        formatted_update_time = format_date(update_time)
        
        # Calculate the percentage
        occupancy_rate = (vehicle_count / total_spaces) * 100
        
        return {
            "vehicle_count": str(vehicle_count), # INT
            "update_time": formatted_update_time, # DATETIME
            "garage_code": garage_code, # STR
            "total_spaces": str(total_spaces), # INT
            "occupancy_rate": str(occupancy_rate)  # FLOAT # Add percentage to the output
        }