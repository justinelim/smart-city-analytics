import asyncio
from scripts.data_stream_simulator import main as data_stream_simulator_main, started_event
from src.data_cleaning.data_cleaner import main as data_cleaner_main

async def main():
    # Start the data stream simulator
    data_stream_simulator_task = asyncio.create_task(data_stream_simulator_main())

    # Optionally wait for a signal that the simulator is operational
    await started_event.wait()

    # Start the data cleaning coordinator
    data_cleaner_task = asyncio.create_task(data_cleaner_main())

    # Wait for both tasks to complete
    await asyncio.gather(
        data_stream_simulator_task,
        data_cleaner_task
    )

if __name__ == "__main__":
    asyncio.run(main())
