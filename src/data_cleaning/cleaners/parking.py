import logging
from src.data_cleaning.data_cleaner import DataCleaner

class ParkingCleaner(DataCleaner):
    primary_key = "garagecode"
    primary_key_idx = 4

    def __init__(self, kafka_topic, json_config, pool):
        super().__init__(kafka_topic, json_config, pool)
        self.logger = logging.getLogger(__name__)

    async def transform_data(self, data: bytes):
        """
        Handle special processing required before inserting the data into the database:
        (1) Enrich with metadata
        """
        logging.debug("Reached transform_data() method of the ParkingCleaner class")
        deserialized_data = self.deserialize_data(data)
        enriched_data = await self.enrich_parking_data(deserialized_data, self.primary_key, self.primary_key_idx)
        # serialized_data = self.serialize_data(enriched_data)
        # logging.debug(f"Type(Enriched data): {type(enriched_data)}")

        return enriched_data
    
    async def enrich_parking_data(self, data: list, primary_key: str, primary_key_idx: int):
        """
        Add parking metadata to the main parking dataset.
        :param data: incoming message or a list of values
        :param primary_key: the primary key column name in the parking_metadata table
        :param primary_key_idx: the index of the primary key in the data list
        :param pool: the async connection pool
        """
        garage_code_value = data[primary_key_idx]  # Get the actual value from data
        logging.debug(f"Garage code value for lookup: {garage_code_value}")
        async with self.pool.acquire() as conn, conn.cursor() as cursor:
            # conn = connect_to_mysql()
            # cursor = conn.cursor()
            query = f"SELECT * FROM smart_city.parking_metadata WHERE {primary_key} = %s"
            
            await cursor.execute(query, (garage_code_value,))
            row = await cursor.fetchone()

            if row:
                # city = row[1]
                # postal_code = row[2]
                # street = row[3]
                # house_number = row[4]
                # latitude = row[5]
                # longitude = row[6]

                # Add columns from `parking_metadata` to the incoming_message
                data.extend(row[1:])  # Assuming the first column is the primary key which is already in `data`
                # enriched_message = tuple(data)

                return data  # Convert tuple back to list for serialization and then return the enriched data in byte format.
            else:
                return data  # If there's no additional data to enrich with, return the original message in byte format.