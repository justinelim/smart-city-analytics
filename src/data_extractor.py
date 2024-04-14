import os
import pandas as pd
import tarfile, zipfile
import json
import glob
import shutil
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def decompress_and_flatten(source_dir, target_dir, csv_pattern, tar_pattern, zip_pattern):
    os.makedirs(target_dir, exist_ok=True)  # Ensure destination directory exists
    
    # Process direct CSV files
    if csv_pattern:
        csv_files = glob.glob(os.path.join(source_dir, csv_pattern))
        logging.info(f"Found {len(csv_files)} CSV files to process.")
        for csv_file_path in csv_files:
            dest_file_path = os.path.join(target_dir, os.path.basename(csv_file_path))
            if not os.path.exists(dest_file_path):
                shutil.copy(csv_file_path, target_dir)
                logging.info(f"Copied {csv_file_path} to {target_dir}")
            else:
                logging.info(f"File {dest_file_path} already exists. Skipping copy.")

    # Process tar files
    if tar_pattern:
        tar_files = glob.glob(os.path.join(source_dir, tar_pattern))
        logging.info(f"Found {len(tar_files)} tar files to process.")
        for tar_file_path in tar_files:
            with tarfile.open(tar_file_path, 'r:gz') as tar:
                tar.extractall(target_dir)
                logging.info(f"Extracted {tar_file_path} to {target_dir}")

    # Process zip files
    if zip_pattern:
        zip_files = glob.glob(os.path.join(source_dir, zip_pattern))
        logging.info(f"Found {len(zip_files)} zip files to process.")
        for zip_file_path in zip_files:
            with zipfile.ZipFile(zip_file_path, 'r') as zipf:
                zipf.extractall(target_dir)
                logging.info(f"Extracted {zip_file_path} to {target_dir}")

    # Flatten the directory by moving all files from subfolders to the destination directory
    for root, dirs, files in os.walk(target_dir, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if file_path.endswith('.csv') and root != target_dir:  # Ensure we're only moving CSV files and they are not in the destination directory already
                target_path = os.path.join(target_dir, name)
                if not os.path.exists(target_path):
                    shutil.move(file_path, target_dir)
                else:
                    os.remove(file_path)  # Remove the duplicate
        # Remove empty directories
        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)

def combine_txt_files_to_csv(target_dir, output_csv_path):
    """Read JSON files and combine into a single DataFrame."""
    combined_data = {}

    for file_name in os.listdir(target_dir):
        if file_name.endswith('.txt'):
            file_path = os.path.join(target_dir, file_name)
            column_name = file_name.replace('.txt', '')

            # Attempt to parse each line as a separate JSON object
            with open(file_path, 'r') as file:
                for line in file:
                    try:
                        data = json.loads(line)  # Parses a JSON object from a string
                        for timestamp, value in data.items():
                            if timestamp not in combined_data:
                                combined_data[timestamp] = {}
                            combined_data[timestamp][column_name] = value
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON from {file_name}: {e}")

    # Convert the dictionary to DataFrame
    combined_df = pd.DataFrame.from_dict(combined_data, orient='index')
    combined_df.index.name = 'timestamp'
    combined_df.reset_index(inplace=True)
    combined_df.to_csv(output_csv_path, index=False)
    logging.info(f"Combined data written to {output_csv_path}")

def load_config(config_path):
    '''
    Load config for all datasets from JSON file
    '''    
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    return config

def load_indiv_config(dataset, json_config):
    dataset_config = json_config.get(dataset, {})
    target_dir = dataset_config.get('target_dir', None)
    csv_file_pattern = dataset_config.get('csv_file_pattern', None)
    tar_file_pattern = dataset_config.get('tar_file_pattern', None)
    zip_file_pattern = dataset_config.get('zip_file_pattern', None)
    return target_dir, csv_file_pattern, tar_file_pattern, zip_file_pattern
    
def main():
    config_path = "/usr/src/app/src/extractor-config.json"
    dataset_list = [
        # "cultural_events",
        # "library_events",
        # "parking",
        # "parking_metadata",
        "pollution",
        # "road_traffic",
        # "social_events",
        # "weather"
        ]
    json_config = load_config(config_path)
    raw_data_path = "/usr/src/app/data/raw/"
    
    for dataset in dataset_list:
        logging.info(f"Start data extraction for {dataset}..")
        target_dir, csv_file_pattern, tar_file_pattern, zip_file_pattern = load_indiv_config(dataset, json_config)

        decompress_and_flatten(raw_data_path, 
                            target_dir, 
                            csv_file_pattern,
                            tar_file_pattern, 
                            zip_file_pattern)
        
        if dataset == "weather":
            output_csv_path = os.path.join(target_dir, f"{dataset}.csv")
            combine_txt_files_to_csv(target_dir, output_csv_path)
        
if __name__ == "__main__":
    main()