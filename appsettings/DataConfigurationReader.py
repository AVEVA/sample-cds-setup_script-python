import json
import logging
import os
from typing import Any

from .DataConfiguration import DataConfiguration


def readDataConfiguration(path: str) -> DataConfiguration:
    """Open and parse the data configuration file"""

    # Try to open the configuration file
    try:
        with open(
            os.path.join(os.path.dirname(__file__), '../' + path),
            'r',
        ) as f:
            return DataConfiguration.fromJson(json.load(f))
    except Exception as error:
        logging.error(f'Error: {str(error)}')
        logging.error(f'Could not open/read data_configuration.json')
        exit()
