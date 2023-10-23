from utils import load_config

class ConfigLoader:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_clean_config(self, dataset):
        config = load_config(self.config_path)
        dataset_config = config.get(dataset, {})
        return dataset_config.get('clean_field_names', []), dataset_config.get('clean_field_types', [])

    def get_processor_class_name(self, dataset):
        config = load_config(self.config_path)
        dataset_config = config.get(dataset, {})
        return dataset_config.get('processor_class')