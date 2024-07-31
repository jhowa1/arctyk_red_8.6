""" arctyk_sql_exec.py     Author: Jack W. Howard   Wellspring Holdings, LLC   Date: 2022-09-17
This software is the property of Wellspring Holdings, LLC and is not to be disclosed, copied, or shared
without the express written consent of Wellspring Holdings, LLC.

This module contains functions to execute SQL statements in Snowflake and return results to Python.

Functions:
    check_table_exists: check if a table exists in Snowflake
    copyinto_execute: execute a Snowflake CopyInto command
    copyinto_export:  export data from Snowflake to a file in a specified folder
    crt_copyinto_message: create standard message from CopyInto results
    crt_put_message: create standard message from PUT results

    extract_bcp: BCP to extract from SQL Server to a data file, eventually to parquet
    extract_bcp_rows_extracted: Reports the number of rows extracted from the BCP standard output
    extract_odbc_dba: DBA.exe to extract data with ODBC to a data file, eventually to parquet
    extract_odbc: ODBC extract to parquet
    extract_odbc: extracts data via ODBC & Arctyk

    fix_path: convert windows path to a python path
    put: put a file to Snowflake
    put_execute: execute a Snowflake PUT command
    py_current_timestamp: Python current timestamp for use in SQL statements in Python
    sf_copyinto: execute a Snowflake CopyInto command
    sf_put: execute a Snowflake PUT command
    sf_get: get file from Snowflake stage to local file system
    sql_execute: execute a SQL statement in Snowflake and return results to Python
    sql_exec_nolog: execute a SQL statement in Snowflake and return results to Python without logging
    truncate_table: truncate a table in Snowflake
    valid_put_options: validate PUT options
    read_sql_from_file: Reads SQL from a file - this avoids the need to escape quotes in the statement
    write_sql_to_file: Writing SQL to a file avoids the need to escape quotes in the SQL


"""

from dataclasses import dataclass, field
import chardet
import logging
import time
import os
import re
import pyarrow.csv as csv
import pyarrow.parquet as pq
from datetime import datetime
import subprocess
from arctyk_setup import ConfigArctyk, setup_logger
from arctyk import export_to_parquet, FileSystem
from arctyk_common import RepoExec
import arctyk_connect_target as snowflake
import arctyk_connect_repository as repo
from snowflake.connector.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)

logger = logging.getLogger(__name__)

config = ConfigArctyk("C:/ProgramData/WhereScape/Modules/WslPython/arctyk_config.yaml")

os_db_locations = config.get_os_db_locations()
log_folder = os_db_locations["log_folder"]
log_objects = config.get_log_objects()
log_file_name = log_objects["log_file_name"]
log_level = log_objects["log_level"]

database_objects = config.get_database_objects()
stage_name = database_objects["stage_name"]
stage_folder = database_objects["stage_folder"]
stage_schema = database_objects["stage_schema"]

license = config.get_license()
license_key = license["license_key"]

# Create a logger
setup_logger(log_folder, log_file_name, log_level)


class S3Bucket:
    """
    Stores the resulting files directly a S3 Bucket.
    """

    def __init__(
        self,
        bucket: str,
        base_file_key: str,
        max_parts_in_upload: int = 10,
    ):
        self.bucket = bucket
        self.base_file_key = base_file_key
        self.max_parts_in_upload = max_parts_in_upload

    def _as_target_json(self):
        return {
            "type": "s3_bucket",
            "bucket": self.bucket,
            "base_file_key": self.base_file_key,
            "max_parts_in_upload": self.max_parts_in_upload,
        }


@dataclass
class SnowflakeDMLRowCounts:
    """A class representing the known row counts Snowflake will return for DML statements.

    Attributes:
    insert_count (int): The number of rows inserted.
    update_count (int): The number of rows updated.
    delete_count (int): The number of rows deleted.
    selected_count (int): The number of rows selected.
    """

    insert_count: int = field(default=None)
    update_count: int = field(default=None)
    delete_count: int = field(default=None)
    selected_count: int = field(default=None)


def check_table_exists(repo_crsr, sx_crsr, table_name):
    """Check if a table exists in Snowflake.

    Args:
        sx_crsr (pypyodbc.Cursor): Snowflake cursor
        table_name (str): Snowflake table name
    """
    check_sql = f"""
    select 1 from INFORMATION_SCHEMA.Tables WHERE TABLE_NAME = '{table_name}';
"""
    try:
        table_exists = False
        test_results = sx_crsr.execute(check_sql)

        for item in test_results:
            if item[0] == 1:
                table_exists = True
        ret_code_fmt = "1"
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    return ret_code_fmt, msg


def py_current_timestamp():
    """Python current timestamp for use in SQL statements in Python.

    Returns:
        current_timestamp: formatted timestamp string of YYYY-MM-DD HH:MM:SS.nnnnnnnnn
    """
    current_time = time.time()
    current_seconds = int(current_time)
    current_nanoseconds = int((current_time - current_seconds) * 1e9)
    current_timestamp = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(current_seconds)
    )
    current_timestamp += f".{current_nanoseconds:09d}"
    return current_timestamp


def truncate_table(crsr, table_name):
    """Truncate table in Snowflake."""
    sql_truncate_stmt = f"truncate table {table_name};"
    try:
        ret_code_fmt = "1"
        msg = f"Table {table_name} truncated successfully"
        crsr.execute(sql_truncate_stmt)
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    return ret_code_fmt, msg


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


def valid_put_options(options_string: str) -> bool:
    valid_keywords = ["PARALLEL", "OVERWRITE", "AUTO_COMPRESS", "SOURCE_COMPRESSION"]
    valid_arguments = {
        "PARALLEL": ["@@"],
        "OVERWRITE": ["TRUE", "FALSE"],
        "AUTO_COMPRESS": ["TRUE", "FALSE"],
        "SOURCE_COMPRESSION": [
            "AUTO_DETECT",
            "GZIP",
            "BZ2",
            "BROTLI",
            "ZSTD",
            "DEFLATE",
            "RAW_DEFLATE",
            "NONE",
        ],
    }
    """ validate PUT option parameters. 

    Returns:
        new_path (str): the converted path
        run_options (str): return the options string without commas
    """
    try:
        options = options_string.upper().split(",")
    except Exception as ConfigurationError:
        logging.warning("No PUT options provided", str(ConfigurationError))
        logging.warning("Set the default PUT option in extended properties")
        logging.warning("Setting overwrite to FALSE")
        options = ["OVERWRITE=FALSE"]

    keyword_count = {}

    for option in options:
        keyword, argument = option.strip().split("=")

        if keyword.strip() not in valid_keywords:
            return (
                False,
                f"""{keyword.strip()} is not a valid keyword. Valid keywords are: {', '.join(valid_keywords)}""",
            )

        if keyword.strip() == "PARALLEL":
            if not argument.strip().isdigit():
                return (
                    False,
                    f"""{keyword.strip()} is not a valid keyword. Valid keywords are: {', '.join(valid_keywords)}""",
                )
        elif keyword.strip() == "OVERWRITE":
            if argument.strip() not in valid_arguments[keyword.strip()]:
                return (
                    False,
                    f"""{keyword.strip()} is not a valid keyword. Valid keywords are: {', '.join(valid_keywords)}""",
                )
        elif argument.strip() not in valid_arguments[keyword.strip()]:
            return (
                False,
                f"""{keyword.strip()} is not a valid keyword. Valid keywords are: {', '.join(valid_keywords)}""",
            )

        keyword_count[keyword.strip()] = keyword_count.get(keyword.strip(), 0) + 1

    for keyword, count in keyword_count.items():
        if count > 1 and keyword != "OVERWRITE":
            return False, keyword

    # convert to string
    run_options = option.replace(",", "")

    return True, run_options


def sf_copyinto(
    db_name: str,
    db_schema: str,
    load_table: str,
    sf_stage: str,
    file_fmt: str,
    copy_into_options: str,
    crsr: object,
    repo_crsr: object,
):
    """Prepare and execute a Snowflake CopyInto command

    Args:
        db_name (str): database name
        db_schema (str): schema name
        load_table (str): table to be loaded
        sf_stage (str): Snowflake stage
        file_fmt (str): file format
        copy_into_options (str): copy into options
        crsr (object): Snowflake cursor
        repo_crsr (object): repository cursor

    Returns:
        pass
    """

    try:
        copy_sql = f"""
COPY INTO {db_name}.{db_schema}.{load_table}
    FROM '{sf_stage}'
    FILE_FORMAT = {file_fmt}
    {copy_into_options}
;         
"""
        ret_code, ret_msg = copyinto_execute(
            repo_crsr=repo_crsr, crsr=crsr, sql=copy_sql
        )
        ret_code_fmt = str(ret_code[0][0])
    except ProgrammingError as err_message:
        logging.debug("Snowflake SQL execution error occurred: ")
        err_msgs = str(err_message).split(": ")
        if len(err_msgs) > 2:
            logging.debug("Snowflake Return Code:%s", err_msgs[0])
            logging.debug("sfqid %s:", err_msgs[1])
            logging.debug(err_msgs[2])
            RepoExec.AuditLog(
                repo_crsr,
                "E",
                "Snowflake SQL execution error has been encountered",
                "db_code3",
                f"{err_msgs[0]}:{err_msgs[1]}:{err_msgs[2]}",
            )
            RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
        else:
            logging.debug("Attempt to format error failed.  Full message follows")
            logging.debug(err_message)
            RepoExec.AuditLog(
                repo_crsr,
                "E",
                "Snowflake SQL execution error has been encountered",
                "db_code3",
                err_message,
            )
            RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", ret_msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ")
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    return ret_code_fmt, ret_msg


def copyinto_export(
    db_name: str,
    db_schema: str,
    export_stmt: str,
    sf_stage: str,
    file_fmt: str,
    file_name: str,
    copy_into_options: str,
    crsr: object,
    repo_crsr: object,
):
    """copy_into for export to file"""
    try:
        copy_into_stmt = f"""
COPY INTO @{db_name}.{db_schema}.{sf_stage}/{file_name}
FROM ({export_stmt})
{copy_into_options}
FILE_FORMAT = (FORMAT_NAME = {file_fmt})"""

        results = crsr.execute(copy_into_stmt)

        for row_counts in results:
            msg="Copy Into for Export executed successfully"
            db_row_msg = f"Exported {row_counts[2]} rows to {db_name}.{db_schema}.{sf_stage}/{file_name}"
            RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)
            logging.debug(f"sfqid {crsr.sfqid} {db_row_msg}")
            ret_code = "1"
            ret_msg = f"Copy Into executed successfully for {db_name}.{db_schema}.{sf_stage}/{file_name}"
    except ProgrammingError as sf_exe_error:
        logging.debug(f"sfqid {sf_exe_error.sfqid}")
        logging.debug(f"sqlstate {sf_exe_error.sqlstate}")
        logging.debug(f"errno {sf_exe_error.errno}")
        logging.debug(f"err_description {sf_exe_error.msg}"),
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "An error during COPY INTO for Export has been encountered",
            "db_code3",
            f"query id {sf_exe_error.sfqid} - {sf_exe_error.msg}",
        )        
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {sf_exe_error.msg}")

    return ret_code, ret_msg, row_counts[2]


def sql_execute_counts(repo_crsr, crsr, sql):
    """Execute a SQL statement and return the results.
       Row counts are integrated into WS audit logging.

    Args:
        repo_crsr (object): repository cursor
        crsr (object): snowflake cursor
        sql (string): sql statement

    Returns:
        fmt_ret_code (string): return code
        fmt_ret_msg (string): return message
    """

    try:
        logging.debug(f"sql_execute: {sql}")
        crsr.execute(sql)
        cursor_description = crsr.description

        result_dict = {}
        row_counts = ""
        if sql.strip().lower().startswith("select"):
            row_counts = str(crsr.rowcount)
            result_dict = {"number of rows selected": row_counts}
        elif sql.strip().lower().startswith("truncate"):
            result_dict = {"truncate success": sql.strip().lower()}
        elif sql.strip().lower().startswith(("insert", "update", "delete", "merge")):
            row_counts = crsr.fetchone()
            for description, count in zip(cursor_description, row_counts):
                name = description.name.lower()
                result_dict[name] = count
        else:
            result_dict = {
                "sql statement succeeded": " ".join(sql.strip().split()[:6]).lower()
            }

        msg = ""
        if "number of rows inserted" in result_dict:
            msg += "Inserted {inserted:,} rows".format(
                inserted=result_dict["number of rows inserted"]
            )
        if "number of rows updated" in result_dict:
            if msg:
                msg += ", "
            msg += "Updated {updated:,} rows".format(
                updated=result_dict["number of rows updated"]
            )
        if "number of rows deleted" in result_dict:
            if msg:
                msg += ", "
            msg += "Deleted {deleted:,} rows".format(
                deleted=result_dict["number of rows deleted"]
            )
        if "number of rows selected" in result_dict:
            if msg:
                msg += ", "
            msg += "Delected {selected:} rows".format(
                selected=result_dict["number of rows selected"]
            )
        if "truncate success" in result_dict:
            if msg:
                msg += ", "
            msg += "Successfully executed {truncate:}".format(
                truncate=result_dict["truncate success"]
            )
        else:
            if msg == "":
                msg += (
                    f"{next(iter(result_dict.values())).upper()} successfully executed"
                )
        msg += "."
        db_row_msg = str(result_dict).replace("'", "`")[:1023]

        ret_code = RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)

        ret_code_fmt = f"{ret_code[0][0]}"

    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ")
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    return (
        ret_code_fmt,
        msg,
        result_dict,
    )


def sql_execute(repo_crsr, crsr, sql):
    ret_code_fmt, msg, *_ = sql_execute_counts(repo_crsr, crsr, sql)
    return ret_code_fmt, msg


def sf_put_parquet(**kwargs):
    path_or_folder = kwargs.get("path_or_folder", None)
    file_pattern = (kwargs.get("file_pattern", None),)
    stage_folder = kwargs.get("stage_folder", None)
    load_file_extension = kwargs.get("load_file_extension", None)
    load_table = kwargs.get("load_table", None)
    put_mode = kwargs.get("put_mode", None)
    put_options = kwargs.get("put_options", None)
    crsr = kwargs.get("crsr", None)
    repo_crsr = kwargs.get("repo_crsr", None)
    path_or_folder = kwargs.get("path_or_folder", None)
    pattern = kwargs.get("pattern", None)

    if not path_or_folder:
        RepoExec.ExitHandler("-2", "No path or folder provided for PUT")
        return

        # Check if the path_or_folder is a directory
    if os.path.isdir(path_or_folder):
        source_path = path_or_folder
        file_pattern = file_pattern
    else:
        source_path = os.path.dirname(path_or_folder)
        file_pattern = os.path.basename(path_or_folder)

    sf_stage = sf_put(
        source_path=source_path,
        file_pattern=file_pattern,
        stage_folder=stage_folder,
        load_file_extension=load_file_extension,
        load_table=load_table,
        put_mode=put_mode,
        put_options=put_options,
        crsr=crsr,
        repo_crsr=repo_crsr,
    )

    return True, sf_stage

def get(
    export_path: str,
    file_name: str,
    export_db: str,
    export_folder: str,
    export_schema: str,
    get_options: str,
    crsr: object,
    repo_crsr: object,
):
    """ Execute GET statement to move file from snowflake to local file system"""
    export_path = fix_path(export_path)
    try:
        results = crsr.execute(
            f"""GET @{export_db}.{export_folder}.{export_schema}/{file_name} 'file://{export_path}'
            {get_options}"""
        )

        for row_counts in results:
            msg="Get for Export executed successfully"
            db_row_msg = f"Get {export_db}.{export_folder}.{export_schema}/{file_name} 'file://{export_path}"       
            RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)            
            logging.debug(f"sfqid {crsr.sfqid}")
            ret_code = "1"
            ret_msg = f"GET executed successfully for {export_db}.{export_folder}.{export_schema}/{file_name}"
    except ProgrammingError as sf_exe_error:
        logging.debug(f"sfqid {sf_exe_error.sfqid}")
        logging.debug(f"sqlstate {sf_exe_error.sqlstate}")
        logging.debug(f"errno {sf_exe_error.errno}")
        logging.debug(f"err_description {sf_exe_error.msg}"),
        ret_code = "-2"
        ret_msg = f"Snowflake SQL execution error: {sf_exe_error.msg}"
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "An error during the GET statement has been encountered",
            "db_code3",
            f"query id {sf_exe_error.sfqid} - {sf_exe_error.msg}",
        )           
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {sf_exe_error.msg}")

    return ret_code, ret_msg


def sf_put(
    source_path: str,
    file_pattern: str,
    stage_folder: str,
    load_file_extension: str,
    load_table: str,
    put_mode: str,
    put_options: str,
    crsr: object,
    repo_crsr: object,
):
    """Prepare and execute a Snowflake PUT command using several parameters.
       Currently, this function requires comma delimited parameters within the
       options string. This will be updated if need is found in the future.

    Args:
        source_path (str): where to find files
        file_pattern (str): wildcard file names are supported
        stage_folder (str): internal Snowflake stage folder
        load_file_extension (str): file extension to be loaded - parquet, csv, etc.
        load_table (str): table to be loaded
        put_mode (str): TABLE or FOLDER - see documentation for details.  This is a
        global variable set in the config.yaml file.
        put_options (str): options to be passed for PUT
        crsr (object): Snowflake cursor
        repo_crsr (object): repository cursor

    Returns:
        pass
    """

    try:
        pass_put, put_options = valid_put_options(put_options)
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_datetime = (
            current_datetime.replace(" ", "_").replace(":", " ").replace("-", " ")
        )

        new_file_pattern = ""
        put_sql = ""
        sf_stage = ""

        if put_mode == "TABLE":
            sf_stage = f"@%{load_table}"
        elif put_mode == "FOLDER":
            sf_stage = f"@{stage_folder}/{load_table}/{formatted_datetime}"

        if load_file_extension.upper() == "PARQUET":
            base_name, old_extension = os.path.splitext(file_pattern)
            new_extension = "." + load_file_extension

            new_file_pattern = file_pattern.replace(old_extension, new_extension)
            put_path = f"{source_path}{new_file_pattern}"
        else:
            new_file_pattern = file_pattern
            put_path = f"{source_path}{new_file_pattern}"

        src_path = fix_path(source_path)
        put_path = f"{src_path}{new_file_pattern}"
        logging.debug(
            f"""the sf_stage is :{sf_stage}, the put_path is :{put_path} {put_options}
              put_sql is :{put_sql}"""
        )
        put_sql = f"""PUT 'file://{put_path}' '{sf_stage}' {put_options};"""
        ret_code, retmsg = put_execute(repo_crsr=repo_crsr, crsr=crsr, sql=put_sql)
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    return True, sf_stage


def copyinto_execute(repo_crsr, crsr, sql):
    """Copy into execute is necessary because Snowflake does not
    support a standard sql response method.

    Args:
        repo_crsr (): repository cursor
        crsr ()): snowflake
        sql (string): SQL containing a Copy Into statement

    Returns:
        something:
    """
    try:
        logging.debug(f"""{sql}""")
        copyinto_results = crsr.execute(sql)

        ret_code, msg = crt_copyinto_message(
            repo_crsr=repo_crsr, results=copyinto_results
        )
        ret_code_fmt = str(ret_code[0][0])
    except ProgrammingError as SF_ProgrammingError:
        final_db_msg = str(SF_ProgrammingError).replace("\\n", "").replace("'", "")
        msg = "CopyInto phase of script failed"
        logging.debug(f"""Snowflake SQL execution error occurred: {final_db_msg} """)
        RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", final_db_msg)
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ")
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    return ret_code_fmt, msg


def crt_copyinto_message(repo_crsr, results):
    rows_loaded_sum = 0
    files_loaded_count = 0

    try:
        for row in results:
            (
                file,
                status,
                rows_parsed,
                rows_loaded,
                error_limit,
                errors_seen,
                first_error,
                first_error_line,
                first_error_character,
                first_error_column_name,
            ) = row

            fmt_rows_loaded = f"{rows_loaded:,}"
            # individual row message
            db_row_msg = f"{file} {status} with {fmt_rows_loaded} rows loaded"
            msg = "Copy Into executed successfully"
            RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)
            rows_loaded_sum += rows_loaded
            files_loaded_count += 1
    except ValueError as copyinto_message:
        msg = "CopyInto did not load any rows"
        logging.debug(msg)
        rows_loaded_sum = 0
        files_loaded_count = 0
        RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", "")

    # all done message
    fmt_rows_sum = f"{rows_loaded_sum:,}"
    if files_loaded_count == 1:
        files_word = "file"
    else:
        files_word = "files"
    final_db_msg = f"A total of {fmt_rows_sum} rows were loaded from {files_loaded_count} {files_word}"
    msg = "CopyInto phase of script is done"
    ret_code = RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", final_db_msg)
    ret_code_fmt = str(ret_code[0][0])

    return ret_code_fmt, final_db_msg


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


def extract_bcp(
    repo_crsr: object,
    extract_sql: str,
    load_table: str,
    work_dir: str,
    source_dsn: str,
    data_file_charset: str,
):
    """Use BCP to extract from SQL Server to a data file, eventually to parquet.
    Note: -D option in BCP changes the Server switch from the name of the server
    to the name of the DSN.   The DSN controls the database to be connected to.

    The field delimiter is set to char[18] since this value does not appear in end user data.

    Args:
        extract_sql (str): SQL to be run by bcp
        load_table (str): name of the table to load
        field_delimiter (str): delimiter to use in the data file
        bcp_server (str): name of the server to connect to
        bcp_db (str): name of the database to connect to
        data_file_encoding (str): encoding of the data file
    Returns:
        ret_code (str): return code
        ret_msg (str): return message
    """
    if len(data_file_charset) == 0:
        data_file_charset = "ACP"

    data_filename = os.path.join(work_dir, load_table) + ".dat"
    char_18 = chr(18)
    bcp_options = f""" -a 32576 -c -C {data_file_charset} -t "{char_18}" -T -D -S {source_dsn} -q"""

    bcp_command = (
        f"""bcp "{extract_sql}" queryout "{data_filename}" {bcp_options}""".replace(
            "\n", " "
        )
    )

    try:
        completed_process = subprocess.run(
            bcp_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        msg = f"BCP completed successfully for {load_table}"
        db_row_msg = extract_bcp_rows_extracted(completed_process.stdout.decode())
        RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)
        ret_code = "1"
        ret_msg = msg
        # Access stdout and stderr
    except subprocess.CalledProcessError as called_process_error:
        logging.debug(f"CalledProcessError: {called_process_error}")
        ret_msg = f"Call to BCP failed: {bcp_command}"
        ret_code = "-2"

    parquet_filename = os.path.join(work_dir, load_table) + ".parquet"
    ro = csv.ReadOptions(autogenerate_column_names=True, skip_rows=0)
    po = csv.ParseOptions(delimiter=chr(18))

    load_table = csv.read_csv(data_filename, read_options=ro, parse_options=po)
    pq.write_table(
        load_table,
        parquet_filename,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    msg = f"Parquet file created for {load_table}"
    db_row_msg = ""
    RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)

    return ret_code, ret_msg


def extract_bcp_rows_extracted(bcp_std_out: str):
    """Reports the number of rows extracted from the BCP standard output

    Args:
        bcp_std_out (str): standard output from the BCP command

    Returns:
        msg: a formatted message with rows extracted
    """
    pattern = r"(\d+) rows (?:successfully bulk-copied|copied)"
    matches = re.findall(pattern, bcp_std_out)
    if matches:
        rows_extracted = "BCP extracted " + "{:,}".format(int(matches[-1])) + " rows"
        return rows_extracted
    else:
        return "BCP extracted 0 rows"


def extract_odbc_dba(
    repo_crsr: object,
    extract_sql: str,
    load_table: str,
    work_dir: str,
    source_dsn: str,
    dsn_arch: str,
    user: str,
    password: str,
):
    """WhereScape DBA.exe to extract data with ODBC to a data file.

    The field delimiter is set to char[18] since this value does not appear in end user data.
    Depending on the ODBC driver, we don't know what the file encoding is going to be,
    so we are flexible and adapt.

    Args:
        reop_crsr (object): cursor to the repository
        extract_sql (str): extract_sql
        load_table (str): name of the table to load
        work_dir (str): working directory
        source_dsn (str): name of the DSN to connect to
        data_file_charset (str): encoding of the data file
        user (str):
        password (str):
    Returns:
        ret_code, ret_msg
    """

    sql_filename = os.path.join(work_dir, load_table) + ".sql_tmp"

    write_sql_to_file(sql=extract_sql, sql_filename=sql_filename)

    data_filename = os.path.join(work_dir, load_table) + ".dat"
    char_18 = chr(18)

    dba_command = f""""C:\\Program Files\\WhereScape\\RED\\dba.exe" /B --meta-dsn-arch {dsn_arch} /Z /O {source_dsn} /u  {user} /P {password} /C "{sql_filename}" /D"{char_18}" /F {data_filename} """

    logging.debug(dba_command)

    try:
        completed_process = subprocess.run(
            dba_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        msg = f"dba completed successfully for {load_table}"
        db_row_msg = completed_process.stdout.decode()
        RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)
        ret_code = "1"
        ret_msg = msg
        # Access stdout and stderr
    except subprocess.CalledProcessError as called_process_error:
        ret_msg = f"Call to dba failed: {dba_command} with error {called_process_error}"
        logging.debug(ret_msg)
        ret_code = "-2"
        return ret_code, ret_msg

    # find file encoding
    with open(data_filename, "rb") as file:
        raw_data = file.read(1024)  # Read first 1024 bytes for discovery

    result = chardet.detect(raw_data)
    encoding = result["encoding"]

    parquet_filename = os.path.join(work_dir, load_table) + ".parquet"
    ro = csv.ReadOptions(autogenerate_column_names=True, skip_rows=0, encoding=encoding)
    po = csv.ParseOptions(delimiter=chr(18))

    load_table = csv.read_csv(data_filename, read_options=ro, parse_options=po)
    pq.write_table(
        load_table,
        parquet_filename,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    ret_msg = f"Parquet file created for {load_table}"
    db_row_msg = ""
    RepoExec.AuditLog(repo_crsr, "I", ret_msg, "db_code3", db_row_msg)

    delete_file(sql_filename=sql_filename)
    delete_file(sql_filename=data_filename)

    return ret_code, ret_msg


def put_execute(repo_crsr, crsr, sql):
    """PUT Execute is necessary because Snowflake does not
    support a standard sql response method.

    Args:
        repo_crsr (): repository cursor
        crsr ()): snowflake crsr
        sql (string): SQL containing PUT or GET statement

    Returns:
        something:
    """
    try:
        logging.debug(f"put_execute: {sql}")
        put_results = crsr.execute(sql)

        ret_code, msg = crt_put_message(repo_crsr=repo_crsr, results=put_results)

        ret_code_fmt = str(ret_code[0][0])
    except (
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
    ) as e:
        msg = str(e).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("Snowflake SQL execution error: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")

    except Exception as unexpected_error:
        msg = str(unexpected_error).replace("\n", " ").replace("'", "`")[:1023]
        logging.debug("An unexpected error occurred: %s", msg)
        RepoExec.AuditLog(
            repo_crsr,
            "E",
            "Snowflake SQL execution error has been encountered",
            "db_code3",
            msg,
        )
        RepoExec.ExitHandler("-2", f"Snowflake SQL execution error: {msg}")
    return ret_code_fmt, msg


def crt_put_message(repo_crsr, results):
    """crt_put_message is a helper function for put_execute

    Args:
        repo_crsr (object): _description_
        results (_type_): Returned from Snowflake PUT or GET statement

    Returns:
        ret_code: return code
        msg: final message
    """
    bytes_loaded_sum = 0
    files_loaded_count = 0
    for row in results:
        (
            source,
            target,
            source_size,
            target_size,
            source_compression,
            target_compression,
            status,
            message,
        ) = row

        fmt_bytes_loaded = f"{target_size:,}"

        bytes_loaded_sum += target_size
        files_loaded_count += 1

        # individual row message
        db_row_msg = f"File {source} with {fmt_bytes_loaded} bytes was PUT with status: {status} {message}."
        msg = "PUT statement executed successfully"
        RepoExec.AuditLog(repo_crsr, "I", msg, "db_code3", db_row_msg)

    fmt_bytes_sum = f"{bytes_loaded_sum:,}"
    # all done message
    if files_loaded_count == 1:
        files_word = "file"
    else:
        files_word = "files"

    final_db_msg = f"A total of {fmt_bytes_sum} bytes were uploaded in {files_loaded_count} {files_word}"
    final_msg = "PUT phase of script is done"
    ret_code = RepoExec.AuditLog(repo_crsr, "I", final_msg, "db_code3", final_db_msg)
    ret_code = str(ret_code[0][0])

    return ret_code, final_msg


def read_sql_from_file(sql_filename: str):
    """
    The bookend fundction to writing sql to a file  - this avoids the need to escape quotes in the sql.

    Args:
        sql_filename (str): name of the file to read from

    Returns:
        sql (str): sql read from the file
    """
    with open(sql_filename, "r") as file:
        sql = file.read()
    return sql


def write_sql_to_file(sql: str, sql_filename: str):
    """
    Writes sql to a file - this avoids the need to escape quotes in the sql.

    Args:
        sql (str): sql to write to file
        sql_filename (str): name of the file to write to
    """
    with open(sql_filename, "w") as file:
        file.write(str(sql))
    sql_written = f"sql written to temporary file {sql_filename}"
    logging.debug(sql_written)
    return sql_written


def exec_arctyk_extract(
    crsr,
    repo_crsr,
    src_conn_str,
    extract_sql,
    file_size_threshold_in_bytes,
    fetch_buffer_size_in_rows,
    work_dir,
    source_table,
    load_file_extension,
    load_table,
    put_options,
    stage_folder,
):
    """
    Executes an ODBC extract, writes batches to Parquet files,
    and uploads them to a specified stage.

    Parameters:
    - crsr: The cursor for executing SQL commands.
    - repo_crsr: The repository cursor for additional database operations.
    - connection_string (str): The ODBC connection string.
    - extract_sql (str): The SQL query to execute for data extraction.
    - file_size_threshold_in_bytes (int): The maximum file size in bytes
        before a new file is created.
    - fetch_buffer_size_in_rows (int): The number of rows to fetch from
        the ODBC connection at a time.
    - work_dir (str): The working directory where Parquet files will be saved.
    - source_table (str): The source table name, used in naming Parquet files.
    - load_file_extension (str): The file extension for the Parquet files.
    - load_table (str): The name of the table to receive data.
    - put_options: The PUT options for the staging of Parquet files.
    - stage_folder: The Snowflake stage folder.

    Returns:
    - None
    """
    export_path = rf"{work_dir}\{source_table}.{load_file_extension}"
    # Read arrow batches from ODBC
    exporter = export_to_parquet(
        connection_string=src_conn_str,
        query=extract_sql,
        output_path=export_path,
        file_size_threshold_in_bytes=int(file_size_threshold_in_bytes),
        fetch_buffer_size_in_rows=int(fetch_buffer_size_in_rows),
        license_key=license_key,
    )

    while True:
        report = exporter.write_file()
        if report is None:
            break
        logging.debug(f"Writing file: {report.path}")
        # PUT the Parquet file to the specified stage
        sf_stage = sf_put_parquet(
            path_or_folder=report.path + "\ ",
            stage_folder=stage_folder,
            load_file_extension=load_file_extension,
            load_table=load_table,
            put_mode="TABLE",
            put_options=put_options,
            crsr=crsr,
            repo_crsr=repo_crsr,
        )

    return True, sf_stage


def arctyk_data_export(**kwargs):
    """
    Export data to a parquet file via bucket or file system.

    This function handles the export of data from a source database to a specified storage type (e.g., file system or S3 bucket).
    It uses various parameters provided as keyword arguments to customize the export process, including connection details, SQL query, and storage configurations.

    Keyword Args:
        src_conn_str (str): The connection string to the source database.
        extract_sql (str): The SQL query to extract the data.
        storage_args (dict): The storage arguments including file name, location, and other specifics.
        target_type (str): The target storage type, e.g., 'file_system' or 'bucket'.
        file_size_threshold_in_bytes (int, optional): The file size threshold in bytes for splitting parquet files. Defaults to 160,000,000 bytes.
        fetch_buffer_size_in_rows (int, optional): The number of rows to fetch per buffer. Defaults to 400,000,000 rows.
        repo_crsr (any, optional): Repository cursor for database operations. Defaults to None.
        crsr (any, optional): Cursor for database operations. Defaults to None.
        put_options (dict, optional): Options for the PUT operation when storing files. Defaults to None.
        load_table (str, optional): Name of the table to load data into. Defaults to None.
        load_file_extension (str, optional): File extension for the load files. Defaults to None.

    Returns:
        tuple: A tuple containing a boolean value indicating the success of the export and the stage folder (sf_stage) if the target storage type is "file_system".
    """
    repo_crsr = kwargs.get("repo_crsr", None)
    crsr = kwargs.get("crsr", None)
    put_options = kwargs.get("put_options", None)
    load_table = kwargs.get("load_table", None).lower()
    load_file_extension = kwargs.get("load_file_extension", None)
    src_conn_str = kwargs.get("src_conn_str", None)
    extract_sql = kwargs.get("extract_sql")
    storage_args = kwargs.get("storage_args")
    target_type = (kwargs.get("target_type"),)
    file_size_threshold_in_bytes = (
        kwargs.get("file_size_threshold_in_bytes", 160000000),
    )  # Default value if not provided
    fetch_buffer_size_in_rows = (
        kwargs.get("fetch_buffer_size_in_rows", 400000000),
    )  # Default value if not provided
    # unpack the storage_args dictionary
    storage_args = kwargs.get("storage_args")
    file_name = storage_args["file_name"]
    storage_location_name = storage_args["storage_location_name"]
    folder_name = storage_args["folder_name"].lower()
    max_parts_in_upload = storage_args["max_parts_in_upload"]

    """ Determin the target storage type and create the target storage object """
    if target_type[0] == "bucket":
        target_store = S3Bucket(
            bucket=storage_location_name,
            base_file_key=f"""{folder_name}/{load_table}/{file_name}""",
            max_parts_in_upload=max_parts_in_upload,
        )
    else:
        target_store = FileSystem(
            output_path=f"""{storage_location_name}/{file_name}"""
        )

    exporter = export_to_parquet(
        connection_string=src_conn_str,
        query=extract_sql,
        target_storage=target_store,
        file_size_threshold_in_bytes=int(file_size_threshold_in_bytes[0]),
        fetch_buffer_size_in_rows=int(fetch_buffer_size_in_rows[0]),
        license_key=license_key,
    )

    while True:
        report = exporter.write_file()
        if report is None:
            break
        logging.debug(f"Writing file: {report.path}")
        # PUT the Parquet file to the specified stage
        if target_type[0] == "file_system":
            sf_stage = sf_put_parquet(
                path_or_folder=report.path + "\ ",
                stage_folder=stage_folder,
                load_file_extension=load_file_extension,
                load_table=load_table,
                put_mode="TABLE",
                put_options=put_options,
                crsr=crsr,
                repo_crsr=repo_crsr,
            )
        else:
            sf_stage = stage_folder

    return True, sf_stage
