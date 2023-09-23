import os
import tarfile, zipfile
import json
import glob
import pandas as pd
import numpy as np
from utils import connect_to_mysql

class DataExtractor:
    def __init__(self, table:str, fields:tuple, destination_dir:str, tar_file_pattern:str=None, zip_file_pattern:str=None) -> None:
        self.table = table
        self.fields = fields
        self.destination_dir = destination_dir
        self.tar_file_pattern = tar_file_pattern
        self.zip_file_pattern = zip_file_pattern

    def decompress(self):
        if self.tar_file_pattern:
            for tar_file_path in glob.glob(self.tar_file_pattern):
                folder_name = os.path.splitext(os.path.basename(tar_file_path))[0]  # Extract folder name from source file path
                destination_subdir = os.path.join(self.destination_dir, folder_name)  # Create subdirectory path within destination_dir
                os.makedirs(destination_subdir, exist_ok=True)  # Create the subdirectory if it doesn't exist
                with tarfile.open(tar_file_path, 'r:gz') as tar_ref:
                    tar_ref.extractall(destination_subdir)

        if self.zip_file_pattern:
            for zip_file_path in glob.glob(self.zip_file_pattern):
                folder_name = os.path.splitext(os.path.basename(zip_file_path))[0]
                destination_subdir = os.path.join(self.destination_dir, folder_name)
                os.makedirs(destination_subdir, exist_ok=True)
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_ref.extractall(destination_subdir)

    def extract_from_csv(self):
        # Create a cursor object
        conn = connect_to_mysql()
        cursor = conn.cursor()

        for root, dirs, files in os.walk(self.destination_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                print('FILE_PATH', file_path)
                df = pd.read_csv(file_path, header=None)
                
                # Determine if the first row matches the expected header values
                has_header = True if tuple(df.iloc[0]) == self.fields else False

                # Iterate over the rows of the DataFrame and insert into the MySQL table
                for index, row in enumerate(df.itertuples(index=False, name=None), start=1):
                    if has_header and index == 1:
                        continue  # Skip the first row if it matches the expected header values

                    values = tuple(np.where(pd.isnull(row), None, row))
                    placeholders = ", ".join(["%s"] * len(values))
                    fields = ', '.join(self.fields)
                    sql = f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders})"
                    cursor.execute(sql, values)

                conn.commit()

        conn.close()

    def extract_from_json(self):
        conn = connect_to_mysql()
        cursor = conn.cursor()
        data = {}  # Initialize dictionary to store the data

        for root, dirs, files in os.walk(self.destination_dir):
            for file_name in files:
                if file_name.startswith('._'):
                    continue
                print('FILE_NAME', file_name)
                file_path = os.path.join(root, file_name)
                print('FILE_PATH', file_path)
                with open(file_path, 'r') as file:
                    for line in file:
                        

                        line = line.strip()
                        if line:
                            file_data = json.loads(line)
                            field_name = os.path.splitext(file_name)[0]  # Extract the field name from the file name
                            # Extract the timestamp-value pairs from the JSON object
                            timestamp_values = file_data.items()
                            # Append the field values to the corresponding field in the data dictionary
                            for timestamp, value in timestamp_values:
                                if timestamp not in data:
                                    data[timestamp] = {}
                                data[timestamp][field_name] = value

        df = pd.DataFrame.from_dict(data, orient='index')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'timestamp'}, inplace=True)

        # if 'aarhus' in file_path:
        #     df['longitude'] = '10.1049860760574'
        #     df['latitude'] = '56.2317206942821'
        # elif 'brasov' in file_path:
        #     df['longitude'] = '10.179336344162948'  # nowhere
        #     df['latitude'] = '56.138322977998705'  # nowhere

        print(df)

        # Iterate over the rows of the DataFrame and insert into the MySQL table
        for index, row in enumerate(df.itertuples(index=False, name=None), start=1):
            values = tuple(np.where(pd.isnull(row), None, row))
            placeholders = ", ".join(["%s"] * len(values))
            fields = ', '.join(self.fields)
            sql = f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders})"
            cursor.execute(sql, values)

            conn.commit()

        conn.close()

if __name__ == '__main__':
    data_sources_dict = {
        # 'cultural_events': {
        #     'table_name': 'cultural_events',
        #     'fields': ('row_id', 'city', 'event_name', 'ticket_url', 'ticket_price', 'timestamp', 'postal_code', 'longitude', 'event_id', 'event_description', 'venue_address', 'venue_name', 'event_date', 'latitude', 'venue_url', 'organizer_id', 'category', 'image_url', 'event_type'),
        #     'destination_dir': 'resources/cultural_events_dataset',
        #     'tar_file_pattern': None,
        #     'zip_file_pattern': None
        # },
        # 'library_events': {
        #     'table_name': 'library_events',
        #     'fields': ('lid', 'city', 'endtime', 'title', 'url', 'price', 'changed', 'content', 'zipcode', 'library', 'imageurl', 'teaser', 'street', 'status', 'longitude', 'starttime', 'latitude', '_id', 'id', 'streamtime'),
        #     'destination_dir': 'resources/library_events_dataset',
        #     'tar_file_pattern': None,
        #     'zip_file_pattern': None
        # },
        'parking': {
            'table_name': 'parking',
            'fields': ('vehiclecount', 'updatetime', '_id', 'totalspaces', 'garagecode', 'streamtime'),
            'destination_dir': 'resources/parking_dataset',
            'tar_file_pattern': None,
            'zip_file_pattern': None
        },
        'parking_metadata': {
            'table_name': 'parking_metadata',
            'fields': ('garagecode', 'city', 'postalcode', 'street', 'housenumber', 'latitude', 'longitude'),
            'destination_dir': 'resources/parking_metadata',
            'tar_file_pattern': None,
            'zip_file_pattern': None
        },
        # 'pollution': {
        #     'table_name': 'pollution',
        #     'fields': ('ozone', 'particullate_matter', 'carbon_monoxide', 'sulfure_dioxide', 'nitrogen_dioxide', 'longitude', 'latitude', 'timestamp'),
        #     'destination_dir': 'resources/extracted_pollution_datasets',
        #     'tar_file_pattern': 'resources/citypulse_pollution_*.tar.gz',
        #     'zip_file_pattern': None
        # },
        # 'road_traffic': {
        #     'table_name': 'road_traffic',
        #     'fields': ('status', 'avgMeasuredTime', 'avgSpeed', 'extID', 'medianMeasuredTime', 'TIMESTAMP', 'vehicleCount', '_id', 'REPORT_ID'),
        #     'destination_dir': 'resources/extracted_road_traffic_datasets',
        #     'tar_file_pattern': 'resources/citypulse_traffic_*.tar.gz',
        #     'zip_file_pattern': 'resources/citypulse_traffic_*.zip'
        # },
        # 'social_events': {
        #     'table_name': 'social_events',
        #     'fields': ('event_type', 'webcast_url', 'event_details', 'webcast_url_alternate', 'event_date'),
        #     'destination_dir': 'resources/social_events_dataset',
        #     'tar_file_pattern': None,
        #     'zip_file_pattern': None
        # },
        # 'weather': {
        #     'table_name': 'weather',
        #     'fields': ('timestamp', 'tempm', 'wspdm', 'dewptm', 'hum', 'pressurem', 'vism', 'wdird'),
        #     'destination_dir': 'resources/extracted_weather_datasets',
        #     'tar_file_pattern': 'resources/raw_weather_data_*.tar.gz',
        #     'zip_file_pattern': 'resources/raw_weather_data_*.zip'
        # },
    }

    for data_source in data_sources_dict:
        extractor = DataExtractor(data_sources_dict[data_source]['table_name'],
                data_sources_dict[data_source]['fields'],
                data_sources_dict[data_source]['destination_dir'],
                data_sources_dict[data_source]['tar_file_pattern'],
                data_sources_dict[data_source]['zip_file_pattern'])
        
        if data_source not in ('cultural_events', 'library_events'):
            extractor.decompress()
        
        if data_source != 'weather':
            extractor.extract_from_csv()
        else:
            extractor.extract_from_json()
    