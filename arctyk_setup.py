""" arctyk_setup.py     Author: Jack W. Howard   Wellspring Holdings, LLC   Date: 2022-10-22
This software is the property of Wellspring Holdings, LLC and is not to be disclosed, 
copied, or shared without the express written consent of Wellspring Holdings, LLC.

This module is used to establish the logger for all modules.

Functions:
    setup_logger: Set up the logger with the specified folder and app name.

"""

import os
import logging
import yaml


def setup_logger(log_folder_name: str, app_name: str, log_level: str):
    """Set up the logger with the specified folder and app name.

    Args:
        log_folder_name (str): The folder in which to store the log file.
        app_name (str): The application name used for the log file name.
    """
    if log_level != "NONE":
        logger = logging.getLogger(__name__)
        logger = logging.getLogger()
        logger.setLevel(log_level)

        # Create a file handler and set the logging level
        full_log_file_name = os.path.join(log_folder_name, app_name)
        file_handler = logging.FileHandler(full_log_file_name)
        file_handler.setLevel(log_level)
        log_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(log_formatter)

        # Add the file handler to the logger
        logger.addHandler(file_handler)

class ConfigArctyk:
    def __init__(self, filename):
        cwd = os.getcwd()
        with open(filename, "r") as f:
            self.config = yaml.safe_load(f)

    def get_os_db_locations(self):
        return self.config["os_db_locations"]

    def get_log_objects(self):
        return self.config["log_objects"]

    def get_database_objects(self):
        return self.config["database_objects"]
    
    def get_license(self):
        return self.config["license"]