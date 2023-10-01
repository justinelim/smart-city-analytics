import requests
import os
from dotenv import load_dotenv

load_dotenv()

elasticsearch_url = os.getenv("ELASTIC_URL")
index_name = "processed_parking"

# Send a DELETE request to delete all documents in the index
purge_url = f"{elasticsearch_url}/{index_name}/_delete_by_query"
query = {
    "query": {
        "match_all": {}  # Match all documents in the index
    }
}

headers = {"Content-Type": "application/json"}
response = requests.post(purge_url, json=query, headers=headers)

if response.status_code == 200:
    print("Deleted all documents in the index.")
else:
    print(f"Failed to delete documents. Status code: {response.status_code}")
