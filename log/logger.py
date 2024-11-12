import logging.config
import os.path

import yaml

logging_config = os.path.join(os.path.dirname(__file__), "logging_config.yaml")
# Load the logging configuration from the YAML file
with open(logging_config, "r") as file:
    config = yaml.safe_load(file)
    logging.config.dictConfig(config)

# Initialize the logger
logger = logging.getLogger("main_logger")
