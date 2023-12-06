import json
from utils import handle_non_serializable_types

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        return handle_non_serializable_types(obj)

class DataCleaner:
    """
    Base class for specific dataset cleaning processors: sets the general structure
    & common functionalities that all specific dataset cleaning processors will use.
    """
    primary_key = None
    primary_key_idx = None
    html_field = None

    def __init__(self, kafka_topic, json_config, pool):
        self.kafka_topic = kafka_topic
        self.json_config = json_config
        self.pool = pool
        # self.raw_field_names = self.json_config["raw_field_names"]

    @staticmethod
    def deserialize_data(data: bytes) -> list:
        print("Data to be serialized:", data)
        return json.loads(data)

    @staticmethod
    def serialize_data(data: list) -> bytes:
        return json.dumps(data, cls=DecimalEncoder).encode('utf-8')
    
    async def transform_data(self, data: bytes) -> list:
        return self.deserialize_data(data)
    
    def get_sql_query(self, data):
        placeholders = ', '.join(["%s"] * len(data))
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        if self.primary_key and self.html_field and self.primary_key_idx is not None:
            update_query = f"UPDATE clean_{self.kafka_topic} SET {self.html_field} = %s WHERE {self.primary_key} = %s"
            return insert_query, data, update_query, self.html_field, self.primary_key_idx
        else:
            return insert_query, data