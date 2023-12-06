from src.data_cleaning.cleaners.cultural_events import CulturalEventsCleaner
from src.data_cleaning.cleaners.parking import ParkingCleaner
from src.data_cleaning.base_cleaner import DataCleaner

def get_cleaner(class_name, *args, **kwargs):
    """
    Factory function that instantiates cleaner objects based on a given name 
    and dynamically loads or injects these classes as needed.
    """
    cleaners = {
        'DataCleaner': DataCleaner,
        'CulturalEventsCleaner': CulturalEventsCleaner,
        'ParkingCleaner': ParkingCleaner,
    }
    cleaner_class = cleaners.get(class_name)
    if cleaner_class:
        # Check if args are provided, otherwise pass kwargs
        if args:
            return cleaner_class(*args, **kwargs)
        else:
            return cleaner_class(**kwargs)
    else:
        raise ValueError(f"No cleaner found for class name: {class_name}")
