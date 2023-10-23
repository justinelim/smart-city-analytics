from src.data_processing.config_loader import ConfigLoader
from src.data_processing.processors.cultural_events import CulturalEventsDataProcessor
from src.data_processing.processors.library_events import LibraryEventsDataProcessor
from src.data_processing.processors.parking import ParkingDataProcessor
from src.data_processing.processors.pollution import PollutionDataProcessor
from src.data_processing.processors.road_traffic import RoadTrafficDataProcessor
from src.data_processing.processors.social_events import SocialEventsDataProcessor
from src.data_processing.processors.weather import WeatherDataProcessor
import logging
"""
Read data from clean Kafka topic, process and send processed data to Elasticsearch.
"""
logger = logging.getLogger()

def main():
    logger.debug('Start processing clean data..')
    # kafka_topic = "clean_cultural_events"
    # kafka_topic = "clean_library_events"
    # kafka_topic = "clean_parking"
    # kafka_topic = "clean_pollution"
    kafka_topic = "clean_road_traffic"
    # kafka_topic = "clean_social_events"
    # kafka_topic = "clean_weather"

    config_path = "config.json"

    config_loader = ConfigLoader(config_path)
    processor_class_name = config_loader.get_processor_class_name(kafka_topic.replace("clean_", ""))
    
    processor_class = globals()[processor_class_name]
    data_processor = processor_class(
        config_path=config_path,
        kafka_topic=kafka_topic
    )
    
    data_processor.load_clean_config()
    data_processor.process_data()


if __name__ == '__main__':
    main()
