import threading
# from scripts.data_stream_simulator import data_stream_simulator
from src.main.python.data_cleaning import clean_raw_data

def main():
    # Create thread objects
    # thread1 = threading.Thread(target=data_stream_simulator)
    thread2 = threading.Thread(target=clean_raw_data)

    # Start the threads
    # thread1.start()
    thread2.start()

    # Wait for the threads to finish
    # thread1.join()
    thread2.join()

if __name__ == "__main__":
    main()
