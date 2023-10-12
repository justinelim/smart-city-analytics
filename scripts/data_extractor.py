import os
import tarfile, zipfile
import json
import glob
import pandas as pd
import numpy as np
from utils import connect_to_mysql, load_config

print('data_extractor is running')

class DataExtractor:
    def __init__(self, dataset_list:list, config_path:str) -> None:
        self.dataset_list = dataset_list
        self.config_path = config_path
        self.config = load_config(self.config_path)

    def extract_data(self):
        
        for dataset in self.dataset_list:
            self.load_raw_config(dataset)
            
            if dataset not in ("cultural_events", "library_events"):
                self.decompress()

            if dataset != 'weather':
                self.extract_from_csv()
            else:
                self.extract_from_json()

    def load_raw_config(self, dataset):
        dataset_config = self.config.get(dataset, {})
        self.raw_table = dataset_config.get("raw_table", None)
        self.raw_field_names = tuple(dataset_config.get("raw_field_names", ()))
        self.destination_dir = dataset_config.get('destination_dir', None)
        self.tar_file_pattern = dataset_config.get('tar_file_pattern', None)
        self.zip_file_pattern = dataset_config.get('zip_file_pattern', None)


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
                has_header = True if tuple(df.iloc[0]) == self.raw_field_names else False

                # Iterate over the rows of the DataFrame and insert into the MySQL table
                for index, row in enumerate(df.itertuples(index=False, name=None), start=1):
                    if has_header and index == 1:
                        continue  # Skip the first row if it matches the expected header values

                    values = tuple(np.where(pd.isnull(row), None, row))
                    placeholders = ", ".join(["%s"] * len(values))
                    fields = ', '.join(self.raw_field_names)
                    sql = f"INSERT INTO {self.raw_table} ({fields}) VALUES ({placeholders})"
                    # print("Executing query:", sql)
                    # print("With values:", values)
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
            fields = ', '.join(self.raw_field_names)
            sql = f"INSERT INTO {self.raw_table} ({fields}) VALUES ({placeholders})"
            cursor.execute(sql, values)

            conn.commit()

        conn.close()

def main():
    dataset_list = [
        # "cultural_events",
        # "library_events",
        # "parking",
        "parking_metadata",
        # "pollution",
        # "road_traffic",
        # "social_events",
        # "weather"
        ]
    config_path = "config.json"

    extractor = DataExtractor(dataset_list=dataset_list, config_path=config_path)
    extractor.extract_data()

if __name__ == '__main__':
    main()