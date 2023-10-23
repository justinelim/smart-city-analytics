import requests
import os
from dotenv import load_dotenv

load_dotenv()

elasticsearch_url = os.getenv("ELASTIC_URL")
index_names = [
    "processed_cultural_events",
    "processed_library_events", 
    "processed_parking",
    "processed_pollution",
    "processed_road_traffic",
    "processed_social_events",
    "processed_weather"
    ]

headers = {"Content-Type": "application/json"}

for index_name in index_names:
    purge_url = f"{elasticsearch_url}/{index_name}/_delete_by_query"
    query = {
        "query": {
            "match_all": {}
        }
    }
    response = requests.post(purge_url, json=query, headers=headers)
    if response.status_code == 200:
        print(f"Deleted all documents in the index {index_name}.")
    else:
        print(f"Failed to delete documents in the index {index_name}. Status code: {response.status_code}")
