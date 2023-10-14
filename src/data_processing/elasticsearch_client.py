# elasticsearch_client.py

import os
import requests

class ElasticsearchClient:
    def __init__(self, base_url=None):
        self.base_url = base_url or os.getenv('ELASTIC_URL')

    def send_to_elasticsearch(self, dataset, value):
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(f"{self.base_url}/processed_{dataset}/_doc/", json=value, headers=headers)
            print(f"Elasticsearch Response: {response.status_code}, {response.text}")
            return response.status_code
        except Exception as e:
            print(f"Error sending to Elasticsearch: {e}")
            return -1  # Return an error code
