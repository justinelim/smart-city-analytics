import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

import threading
from scripts import data_extractor
# from scripts.data_stream_simulator import data_stream_simulator
# from src.data_cleaning import clean_raw_data
# from src.data_processing import process_clean_data


def main():
    # Create thread objects
    thread1 = threading.Thread(target=data_extractor.main)
    # thread2 = threading.Thread(target=data_stream_simulator)
    # thread3 = threading.Thread(target=clean_raw_data.main)
    # thread4 = threading.Thread(target=process_clean_data.main)

    # Start the threads
    thread1.start()
    # thread2.start()
    # thread3.start()
    # thread4.start()


    # Wait for the threads to finish
    thread1.join()
    # thread2.join()
    # thread3.join()
    # thread4.join()

if __name__ == "__main__":
    main()
