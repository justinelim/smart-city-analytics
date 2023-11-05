import sys
import os
import asyncio

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from scripts.data_stream_simulator import main as data_stream_simulator_main

if __name__ == "__main__":
    asyncio.run(data_stream_simulator_main())
