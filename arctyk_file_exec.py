""" arctyk_file_exec.py     Author: Jack W. Howard   Wellspring Holdings, LLC   Date: 2022-09-17
This software is the property of Wellspring Holdings, LLC and is not to be disclosed, copied, or shared
without the express written consent of Wellspring Holdings, LLC.

This module contains functions to process files for import/export to Snowflake.

Functions:
    compress_file: Compresses a single file using gzip or parquet
    compress_files_concurrently: Compresses files concurrently using gzip or parquet
    compress_files_in_folder: Compresses files in a folder using gzip or parquet concurrently
    create trigger_file: Creates a file with the current timestamp and row count
    delete_file: Deletes a temporary file
    delete_source_files: Delete source files after they have been compressed and uploaded to Snowflake
    find_files: Finds files in a directory
    fix_path: Converts windows path to a python path. 
    generate_filename_with_datetime: Generate a new filename with date/time components based on the pattern.
    load_files_polars: Converts csv files to parquet
    move_files_to_archive: Move files to an archive directory
    parse_path: Split a path into its components
    set_wait_file: Supports waiting for a trigger or source file, as specified by the user.
    wait_for_file: Wait for a file to arrive in a directory, based on check_exist and wait_response
    

"""

import datetime
import time
import logging
import os
import glob
import re
import shutil
import polars as pl
import fnmatch
import gzip
import concurrent.futures
from multiprocessing import freeze_support
from arctyk_setup import ConfigArctyk, setup_logger
from arctyk_common import RepoExec

logger = logging.getLogger(__name__)
config = ConfigArctyk("C:/ProgramData/WhereScape/Modules/WslPython/arctyk_config.yaml")

log_objects = config.get_log_objects()
file_object = config.get_os_db_locations()
log_folder = file_object["log_folder"]
log_file_name = log_objects["log_file_name"]
log_level = log_objects["log_level"]
db_objects = config.get_database_objects()


# Create a logger
setup_logger(log_folder, log_file_name, log_level)


def create_trigger_file(
    folder_path: str, filename: str, delimiter: str, row_count: int
):
    """
    Creates or overwrites a file in the specified folder with one record containing
    the current timestamp and an integer value, separated by the specified delimiter.

    :param folder_path: The path to the folder where the file will be created or overwritten.
    :param filename: The name of the file to be created or overwritten.
    :param integer_value: The integer value to be included in the record.
    :param delimiter: The delimiter to separate the timestamp and integer value.
    """
    try:
        # Ensure the folder exists
        os.makedirs(folder_path, exist_ok=True)

        current_timestamp = datetime.datetime.now().isoformat()
        record = f"{current_timestamp}{delimiter}{row_count}"

        trigger_filename = generate_filename_with_datetime(
            filename=filename,
        )
        # Set the initial sequence number
        sequence_number = 1
        sequence_string = r"SEQUENCE"

        trigger_filename = trigger_filename.replace(
            sequence_string, str(sequence_number)
        )
        trigger_filename = trigger_filename.replace("$", "")

        # Construct the full file path
        full_file_path = os.path.join(
            str(folder_path).strip(), str(trigger_filename).strip()
        )

        with open(full_file_path, "w", encoding="UTF-8") as file:
            file.write(record)

        ret_code = "1"
        ret_msg = (
            f"File '{full_file_path}' created or overwritten with record: {record}"
        )
        logging.debug(
            f"File '{full_file_path}' created or overwritten with record: {record}"
        )
    except os.error as e:
        logging.error(f"An error occurred while creating or overwriting the file: {e}")
        ret_code = "-2"
        ret_msg = f"OS error creating the trigger file {filename} with error: {str(e)}"
        RepoExec.ExitHandler(
            "-2", f"OS error creating the trigger file {filename} with error: {str(e)}"
        )

    return ret_code, ret_msg


def delete_file(filename: str):
    """
    Deletes a temporary file
    """
    if os.path.exists(filename):
        os.remove(filename)
        logging.debug(f"Deleted temporary file: {filename}")
    else:
        logging.debug(
            f"The Delete_file function did not find temporary file: {filename}"
        )


def find_files(folder, name_pattern, extension):
    """
    find_files is a function to find files in a directory.

    Args:
        folder (str): the folder to search
        name_pattern (str): the pattern to match
        extension (str): the extension to match

    Returns:
        file_info (list): a list of files matching the pattern
    """

    folder = fix_path(folder)

    files = glob.glob(os.path.join(folder, f"{name_pattern}{extension}"))
    file_info = []

    for file in files:
        file_name = os.path.basename(file)
        file_info.append((folder, file_name))

    return file_info if file_info else False


def set_wait_file(trigger_file_pattern: str, file_pattern: str, source_path: str):
    """If a trigger file is specified, then we set this as the wait filename

    Args:
        trigger_file_pattern (str): Name of the trigger file
        file_pattern (str): Name of the source file when no trigger is given
        source_path (str): The path we find it in

    Returns:
        wait_file_path (str): the name of the thing we are waiting for.
    """
    if len(trigger_file_pattern) > 0:
        wait_filename = trigger_file_pattern
    else:
        wait_filename = file_pattern

    wait_file_root_name, wait_file_extension = os.path.splitext(wait_filename)

    return wait_file_root_name, wait_file_extension


def generate_filename_with_datetime(filename: (str)):
    """
    Generate a new filename with date/time components based on the provided template.

    Args:
        filename (str): The original filename.

    Returns:
        str: The new filename with date/time components.
    """
    current_time = time.strftime("%Y%m%d%H%M%S")
    yy_string = "YYYY"
    mm_string = "MM"
    dd_string = "DD"
    hh_string = "HH"
    mi_string = "MI"
    ss_string = "SS"
    filename_string = "FILENAME"

    # Replace FILENAME with the original filename
    new_filename = filename.replace(filename_string, os.path.splitext(filename)[0])

    # Generate the new filename with date/time components
    new_filename = new_filename.replace(yy_string, current_time[:4])
    new_filename = new_filename.replace(mm_string, current_time[4:6])
    new_filename = new_filename.replace(dd_string, current_time[6:8])
    new_filename = new_filename.replace(hh_string, current_time[8:10])
    new_filename = new_filename.replace(mi_string, current_time[10:12])
    new_filename = new_filename.replace(ss_string, current_time[12:14])
    new_filename = new_filename.replace("$", "")

    return new_filename


def move_files_to_archive(file_pattern, source_path, archive_path, archive_filename):
    """
    Move files to an archive directory.

    Args:
        file_pattern (str): File pattern to match, possibly containing a wildcard.
        source_path (str): Source path to find files in.
        archive_path (str): Archive path to move files to. Create the archive_path directory if it does not exist.
        archive_filename (str): Archive filename to use.

    Raises:
        ValueError: If the file_pattern does not contain an extension.
    """

    archive_path = fix_path(archive_path)
    source_path = fix_path(source_path)
    if not os.path.exists(archive_path):
        logging.debug("Archive path '%s' was not found; created it", archive_path)
        os.makedirs(archive_path)

    # Find files matching the file_pattern in the source_path
    files_to_move = glob.glob(os.path.join(source_path, file_pattern))

    # Set the initial sequence number
    sequence_number = 1
    sequence_string = "$SEQUENCE$"

    # Move each file to the archive_path
    for file_to_move in files_to_move:
        # Get the file name from the file path
        filename = os.path.basename(file_to_move)

        # Generate the new filename with unique sequence number and date/time components
        new_filename = generate_filename_with_datetime(archive_filename, filename)
        new_filename = new_filename.replace(sequence_string, str(sequence_number))
        new_filename = new_filename.replace("$", "")

        # Increment the sequence number
        sequence_number += 1

        # Generate the destination path in the archive_path directory
        destination_path = os.path.join(archive_path, new_filename)

        # Move the file to the archive_path
        shutil.move(file_to_move, destination_path)
        logging.debug("Moved '%s' to '%s'", file_to_move, destination_path)


def delete_source_files(
    file_pattern: str, source_path: str, compress_file_extension: str
):
    """Delete source files after they have been compressed and uploaded to Snowflake

    Args:
        file_pattern (str): source file pattern to delete
        source_path (str): source path to delete files from
        compress_file_extension (str): compressed file extension

    Raises:
        ValueError: If file_pattern does not contain an extension
    """
    files_to_delete = []
    source_path = fix_path(source_path)
    # Extract the file extension from the file_pattern
    _, source_file_extension = os.path.splitext(file_pattern)
    if source_file_extension == "":
        raise ValueError("file_pattern should contain an extension")

    remove_leading_dot = re.sub(r"^\.", "", compress_file_extension)
    compressed_file_extension = f".{remove_leading_dot}"

    # Convert file_extensions to a list if it is a string
    file_extensions = [source_file_extension, compress_file_extension]

    # Find files matching the file_pattern in the source_path
    csv_files = glob.glob(os.path.join(source_path, file_pattern))
    files_to_delete.extend(csv_files)

    # Find the compressed files in the source_path
    compressed_file_pattern = re.sub(r"\.\w+$", compressed_file_extension, file_pattern)
    compressed_files = glob.glob(os.path.join(source_path, compressed_file_pattern))
    files_to_delete.extend(compressed_files)

    # Delete each file - the file may have been moved, so no error if not found
    for file_to_delete in files_to_delete:
        # Check if the file has one of the desired extensions
        if any(
            file_to_delete.lower().endswith(extension) for extension in file_extensions
        ):
            try:
                os.remove(file_to_delete)
                logging.debug("Deleted source file'%s'", file_to_delete)
            except OSError:
                logging.debug("File '%s' not found", file_to_delete)


def wait_for_file(
    check_exist: str,
    wait_response: str,
    source_path: str,
    wait_time: int,
    trigger_file_pattern: str,
    file_pattern: str,
):
    """
    wait_for_file is a function to wait for a file to arrive in a directory, based on check_exist and wait_response.

    The Check_exist is generated by RED, which would otherwise be a boolean value.   Check_exist as false contains
    the RED assumption the file will be present when the process starts.  Check_exist as true means we will
    wait for wait_time and send a response based on wait_response.

    Args:
        check_exist (str): is a string that will be converted to a boolean
        wait_response (str): is a string that will be converted to a boolean
        source_path (str):  is the path to the folder where the file is expected
        wait_time (int): is the number of seconds to wait for the file
        trigger_file_pattern (str): is the name of the trigger file
        file_pattern (str): is the name of the file to wait for
    """

    source_path = fix_path(source_path)

    # since first time in, if the source path is wrong, exit with error
    if not os.path.exists(source_path):
        ret_code = "-3"
        ret_msg = f"Folder {source_path} was not found."
        logging.debug(ret_msg)
        return ret_code, ret_msg

    # Set the wait filename
    wait_filename, wait_file_extension = set_wait_file(
        trigger_file_pattern=trigger_file_pattern,
        file_pattern=file_pattern,
        source_path=source_path,
    )

    logging.debug(f"Wait time for {wait_filename} is {wait_time} seconds")

    start_time = time.time()

    result = find_files(source_path, wait_filename, wait_file_extension)

    if result:
        # The file arrived to the party.  Better late than never.
        ret_msg = f"File {wait_filename} was found."
        logging.debug(ret_msg)
        ret_code = "1"

        return ret_code, ret_msg
    else:
        if check_exist == "false":
            ret_code = "-3"
            ret_msg = f"File {wait_filename}{wait_file_extension} was not found with check_exist = false in {source_path}."
            logging.debug(ret_msg)
            return ret_code, ret_msg
        else:
            # is the file thereNow we count down the wait time, and check for the file.
            while True:
                elapsed_time = time.time() - start_time
                elapsed_time = round(
                    elapsed_time
                )  # Round elapsed_time to the nearest integer

                if elapsed_time < wait_time:
                    result = find_files(source_path, wait_filename, wait_file_extension)

                    if result:
                        # The file arrived to the party.  Better late than never.
                        ret_msg = f"File {wait_filename} was found."
                        logging.debug(ret_msg)
                        ret_code = "1"
                        return ret_code, ret_msg
                    else:
                        remaining_time = wait_time - elapsed_time
                        logging.debug(
                            f"Elapsed time: {elapsed_time} seconds, remaining time to wait is now {remaining_time} seconds"
                        )
                else:
                    # Maximum wait time expired and no file was found
                    logging.debug(
                        f"Maximum wait time of {wait_time} seconds expired and no file matching pattern {file_pattern} was found."
                    )
                    ret_msg = f"Checking for file Exist is True and Action when Wait Limit Reached is specified as {wait_response}."
                    logging.debug(ret_msg)
                    if wait_response == "Error":
                        ret_code = "-2"
                        ret_msg = ret_msg
                        return ret_code, ret_msg
                    elif wait_response == "Fatal Error":
                        ret_code = "-3"
                        ret_msg = ret_msg
                        return ret_code, ret_msg
                    elif wait_response == "Warning":
                        ret_code = "-1"
                        ret_msg = ret_msg
                        return ret_code, ret_msg
                    else:
                        ret_code = "-3"
                        ret_msg = f"Invalid wait_response value of {wait_response} was specified and we reached the wait time limit"
                        return ret_code, ret_msg

                time.sleep(5)


def parse_path(path):
    """parse_path method to split a path into its components.

    Args:
        path (str): the path to parse

    Returns:
        directory (str): the directory
        filename (str): the filename
        file_extension (str): the file extension
    """

    # Split the path into directory and filename
    directory, filename = os.path.split(path)

    # Check if the filename contains a wildcard *
    if "*" in filename:
        # Extract the file extension from the path
        file_extension = os.path.splitext(path)[1]
    else:
        # Split the filename into name and extension
        filename, file_extension = os.path.splitext(filename)

        # Check if the extension is provided
        if not file_extension:
            raise ValueError(
                "File extension must be provided: new file extensions are created during processing"
            )

    return directory, filename, file_extension


def fix_path(directory: str):
    """converts windows path to a python path.
       expects a trailing blank space to avoid escaping the closing quote.

    Args:
        directory (str): the full path

    Returns:
        new_path (str): the converted path
    """
    new_path = directory.replace("\\", "/").rstrip()
    return new_path


def load_files_polars(
    source_path: str,
    file_pattern: str,
    load_table: str,
    field_delimiter: str,
    has_header: bool,
    quote_char: str,
    dtypes: str,
    polars_options: str,
):
    """load_files_polars method to converts csv files to parquet.

    Args:
        source_path (str): location of the csv file(s)
        file_pattern (str): file name pattern to match
        load_table (str): the table name to be loaded
        field_delimiter (str): a single character delimiter
        has_header (str): Due to the way WhereScape handles boolean values, this is a string
                          which will be converted to a boolean
        quote_char (str): a single character quote character
        dtypes (str): a string of column names and types
        polars_options (str): a string of polars options
    Returns:
    """
    has_header = has_header.lower() == "true"
    load_table = load_table.upper()
    source_path = fix_path(source_path)
    file_ext = parse_path(file_pattern)[2]

    # prepare the dtypes mapping
    dtypes_str = dtypes.replace(" ", "")
    dtypes_str = dtypes_str[1:-1]
    dtypes_list = dtypes_str.split(",")

    # Create a mapping for dtypes
    dtypes_mapping = {}
    for dtype in dtypes_list:
        # Remove leading/trailing whitespaces and quotes
        column_name = dtype.strip().strip("'")
        # Add the key-value pair to the mapping
        dtypes_mapping[column_name] = pl.Utf8

    # prepare the polars options
    parameters = {}
    try:
        param_value_pairs = polars_options.split(",")
        for pair in param_value_pairs:
            # Split each pair by '=' to get the parameter and value
            param, value = pair.strip().split("=")
            # Evaluate the value using eval() to handle different data types
            parameters[param.strip()] = eval(value.strip())

    except ValueError:
        logging.debug("optional polars option not passed")
        pass

    for filename in os.listdir(source_path):
        if fnmatch.fnmatch(filename, file_pattern) and filename.endswith(file_ext):
            full_file_path = os.path.join(source_path, filename)

            # Read the CSV file into a PyArrow table
            table = pl.read_csv(
                full_file_path,
                has_header=has_header,
                separator=field_delimiter,
                quote_char=quote_char,
                dtypes=dtypes_mapping,
                **parameters,
            )

            new_filename = os.path.splitext(filename)[0] + ".parquet"
            new_file_path = os.path.join(source_path, new_filename)
            table.write_parquet(new_file_path)
            logging.debug(f"{full_file_path} was compressed to {new_filename}")

    return True


# pylint: disable=W0613,R0913
def compress_file(
    filename: str,
    source_path: str,
    has_header: str,
    separator: str,
    quote_char: str,
    compress_method: str,
):
    """Compresses a single file using gzip or parquet

    Args:
        filename (str): The name of the file to compress
        source_path (str): the folder containing the files to be compressed
        separator (str): the field delimiter
        quote_char (str): the quote character
    """
    full_file_path = os.path.join(source_path, filename)

    has_header_bool = has_header.lower() == "true"

    if compress_method.lower() == "gzip":
        compressed_file_path = full_file_path + ".gz"
        os.makedirs(os.path.dirname(compressed_file_path), exist_ok=True)
        with open(full_file_path, "rb") as f_in:
            with gzip.open(compressed_file_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    elif compress_method.lower() == "parquet":
        table = pl.read_csv(
            full_file_path,
            has_header=has_header_bool,
            separator=",",
            quote_char='"',
        )

        new_filename = os.path.splitext(filename)[0] + ".parquet"
        new_file_path = os.path.join(source_path, new_filename)
        table.write_parquet(new_file_path)

    else:
        logging.debug("No compression method specified - exiting")
        return

    return


def compress_files_concurrently(
    compress_method: str,
    file_list: list,
    source_path: str,
    has_header: str,
):
    """Compresses files concurrently using gzip or parquet.
        Args are passed to the compress_file function to avoid global variables.

    Args:
        file_list (list): a list of files to compress in the source folder as a batch
        source_path (str): the folder containing the files to be compressed
    """
    separator = chr(18)
    quote_char = chr(17)

    freeze_support()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for filename in file_list:
            executor.submit(
                compress_file,
                filename,
                source_path,
                has_header,
                separator,
                quote_char,
                compress_method,
            )


def compress_files_in_folder(
    compress_method: str,
    source_path: str,
    has_header: str,
    file_pattern: str,
    files_per_batch: str,
):
    """Compresses files in a folder using gzip or parquet concurrently
        according to the number of cores available on the machine.

    Args:
        Configure the appropriate values for the following variables:
        source_path: The folder containing the files to be compressed
        EXTENSION: The file extension of the files to be compressed
        COMPRESS_METHOD: The compression method to use. Valid values are 'gzip' and 'parquet'
        files_per_batch: The number of files to compress in each batch
    """

    source_path = fix_path(source_path)

    file_list = [
        os.path.join(source_path, file)
        for file in os.listdir(source_path)
        if fnmatch.fnmatch(file, file_pattern)
    ]

    if len(files_per_batch) == 0:
        files_per_batch_int = int(1)
    else:
        files_per_batch_int = int(files_per_batch)

    batches = [
        file_list[i : i + files_per_batch_int]
        for i in range(0, len(file_list), files_per_batch_int)
    ]

    for file_batch in batches:
        compress_files_concurrently(
            compress_method=compress_method,
            file_list=file_batch,
            source_path=source_path,
            has_header=has_header,
        )
