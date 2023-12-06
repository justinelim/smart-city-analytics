import logging
from src.data_cleaning.base_cleaner import DataCleaner
from utils import convert_unix_timestamp

class CulturalEventsCleaner(DataCleaner):
    ticket_price_field_idx = 4
    timestamp_field_idx = 5

    def __init__(self, kafka_topic, json_config, pool):
        super().__init__(kafka_topic, json_config, pool)
        self.logger = logging.getLogger(__name__)

    async def transform_data(self, data):
        """
        Handle special processing required before inserting the data into the database:
        (1) Convert Unix timestamp values to human-readable timestamp (`timestamp`)
        (2) Calculate the average ticket price

        """
        
        deserialized_data = self.deserialize_data(data)
        transformed_data = convert_unix_timestamp(deserialized_data, self.timestamp_field_idx)
        transformed_data = self.calc_avg_price(transformed_data, self.ticket_price_field_idx)
        logging.debug(f"Data in Cultural transform_data: {transformed_data}")
        return transformed_data
    
    def calc_avg_price(self, data: list, ticket_price_field_idx):
        """
        (1) Clean and parse ticket prices into numerical data type
        (2) Calculate the average ticket price for price ranges
        (3) Map 'Gratis' to 0
        (4) Map 'Billet' to null
        """
        ticket_price = data[ticket_price_field_idx]
        
        # If the ticket price contains a "-", it's treated as a range.
        if '-' in ticket_price:
            # Remove the 'DKK' suffix and split by '-'
            min_price_str, max_price_str = ticket_price.replace('DKK', '').split('-')
            # Convert the extracted prices to float and compute the average
            min_price, max_price = float(min_price_str.strip()), float(max_price_str.strip())
            avg_price = (min_price + max_price) / 2
            data[ticket_price_field_idx] = avg_price
        elif 'DKK' in ticket_price:
            clean_price = float(ticket_price.replace('DKK', '').strip())
            data[ticket_price_field_idx] = clean_price
        elif ticket_price.lower() == 'gratis':
            data[ticket_price_field_idx] = 0.0
        elif ticket_price.lower() == 'billet':
            data[ticket_price_field_idx] = None
        
        return data