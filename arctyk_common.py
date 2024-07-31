"""
Script Name :    arctyk_common.py
Created on  :    September 17, 2022
Author      :    Jack Howard
_______________________________________________________________________________
Purpose     :    Common module for repository callable procedures

Details:  PYODBC is used to facilitate cross database performance.

@Wellspring Holdings LLC - All rights reserved
"""

import os
import sys
import logging
from ctypes import *
import arctyk_connect_repository as repo
from pyodbc import ProgrammingError

crsr = repo.crt_cursor()

logger = logging.getLogger(__name__)

class RepoExec:
    """Interactions with the WhereScape Repository
    See documentation section Callable Routines for full details"""

    def __init__(self):
        self.errors = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        for error_found in self.errors:
            logging.exception(error_found)
        return True

    @staticmethod
    def WsParameterRead(parameter_name: str):
        """Reads a parameter from the repository
        See documentation section Callable Routines for full details


        """

        sql = f"""
        DECLARE @out varchar(max),@out1 varchar(max);
        EXEC WsParameterRead
        @p_parameter = {parameter_name} 
        ,@p_value = @out OUTPUT
        ,@p_comment=@out1 OUTPUT;
        SELECT @out AS p_value,@out1 AS p_comment;"""
        try:
            crsr.execute(sql)
            parm_value = crsr.fetchone()
            crsr.commit()
        except Exception as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )
            logging.debug("A PYODBC DataError occurred:", str_ODBC_error_msg)

        crsr.execute(sql)

        return parm_value

    @staticmethod
    def WsParameterWrite(
        parameter_name: str, parameter_value: str, parameter_comment: str
    ):
        """Writes a parameter in the repository
        See documentation section Callable Routines for full details

        WsParameterWrite -ParameterName "CURRENT_DAY" -ParameterValue "Monday" -ParameterComment "The current day of the week"
        """
        sql = f"""
        SET NOCOUNT ON;
        DECLARE @out nvarchar(max);
        EXEC  @out=WsParameterWrite
        @p_parameter = '{parameter_name}'
        , @p_value = '{parameter_value}'
        , @p_comment  = '{parameter_comment}';
        SELECT @out AS return_value;"""
        try:
            crsr.execute(sql)
            the_result = crsr.fetchall()
            crsr.commit()
        except Exception as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )
            logging.debug("A PYODBC DataError occurred:", str_ODBC_error_msg)
        return the_result

    @staticmethod
    def AuditLog(crsr, status_code: str, message: str, db_code: str, db_message: str):
        """
        This function is used to write audit records to the WhereScape Work Table. It takes in the following parameters:
        - crsr: a cursor object used to execute SQL statements
        - status_code: a string representing the status code of the audit record
        - message: a string representing the message of the audit record
        - db_code: a string representing the database code of the audit record
        - db_message: a string representing the database message of the audit record

        The function retrieves the following environment variables:
        - WSL_SEQUENCE: a string representing the sequence of the current job
        - WSL_JOB_NAME: a string representing the name of the current job
        - WSL_TASK_NAME: a string representing the name of the current task
        - WSL_JOB_KEY: a string representing the key of the current job
        - WSL_TASK_KEY: a string representing the key of the current task

        The function replaces single quotes and newlines in the message and db_message parameters with double quotes and spaces,
        then executes a SQL statement to insert the audit record into the WhereScape Work Table and returns the result.
        """

        try:
            sequence = os.environ["WSL_SEQUENCE"]
        except KeyError:
            sequence = -1
        try:
            job_name = os.environ["WSL_JOB_NAME"]
        except KeyError:
            job_name = "Job Name Not Found"
        try:         
            task_name = os.environ["WSL_TASK_NAME"]
        except KeyError:
            task_name = "Task Name Not Found"            
        try:
            job_id = os.environ["WSL_JOB_KEY"]
        except KeyError:
            job_id = -1
        try:
            task_id = os.environ["WSL_TASK_KEY"]
        except KeyError:
            task_id = -1


        clean_message = str(message).replace("'", "''").replace("\\n", " ")
        clean_db_message = str(db_message).replace("'", "''").replace("\\n", " ")

        the_result = ""
        sql = f"""
        SET NOCOUNT ON;
        DECLARE @out nvarchar(max);
        EXEC  @out=WsWrkAudit
        @p_status_code = '{status_code}' 
        , @p_job_name = '{job_name}'    
        , @p_task_name = '{task_name}'
        , @p_sequence = '{sequence}'   
        , @p_message   = '{clean_message}' 
        , @p_db_code  = '{db_code}' 
        , @p_db_msg = '{clean_db_message}'  
        , @p_task_key  = {task_id}  
        , @p_job_key  = {job_id};
        SELECT @out AS return_value;"""

        try:
            crsr_results = crsr.execute(sql)
            the_result = crsr.fetchall()
            crsr.commit()
        except Exception as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )

            logging.debug("A PYODBC DataError occurred:", str_ODBC_error_msg)
            logging.debug(
                "These are inputs to met:messLogage:%s db_message,%s",
                message,
                db_message,
            )
        return the_result

    @staticmethod
    def WsWrkError(crsr, status_code: str, message: str, db_code: str, db_message: str, message_type: str
    ):
        """
        WhereScape Error API

        This function is used to log errors in a WhereScape project. It takes in several parameters
        related to the error and logs them in the project's error log.

        Args:
            status_code (str): The status code of the error.
            message (str): The error message.
            db_code (str): The database error code.
            db_message (str): The database error message.
            message_type (str): The type of error message.

        Returns:
            string: The result of the error log.
        """

        try:
            sequence = os.environ["WSL_SEQUENCE"]
        except KeyError:
            sequence = -1
        try:
            job_name = os.environ["WSL_JOB_NAME"]
        except KeyError:
            job_name = "Job Name Not Found"
        try:         
            task_name = os.environ["WSL_TASK_NAME"]
        except KeyError:
            task_name = "Task Name Not Found"            
        try:
            job_id = os.environ["WSL_JOB_KEY"]
        except KeyError:
            job_id = -1
        try:
            task_id = os.environ["WSL_TASK_KEY"]
        except KeyError:
            task_id = -1


        clean_message = message.replace("'", "''").replace("\\n", " ")
        clean_db_message = db_message.replace("'", "''").replace("\\n", " ")
        the_result=""

        sql = f"""
        SET NOCOUNT ON;
        DECLARE @out nvarchar(max);
        EXEC @out=WsWrkError
        @p_status_code = '{status_code}'
        , @p_job_name = '{job_name}'   
        , @p_task_name = '{task_name}'
        , @p_sequence = '{sequence}'
        , @p_message   = '{clean_message}'
        , @p_db_code  = '{db_code}'
        , @p_db_msg    = '{clean_db_message}'
        , @p_task_key  = {task_id}
        , @p_job_key  = {job_id}
        , @p_msg_type   = '{message_type}';
        SELECT @out AS return_value;"""
        try:
            crsr.execute(sql)
            the_result = crsr.fetchall()
            crsr.commit()
        except Exception as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )
            logging.debug("A PYODBC error occurred calling WsWrkError:", str_ODBC_error_msg)

        return the_result

    @staticmethod
    def WsJobRelease(job_name: str):
        """Releases a held or waiting job
        See documentation section Callable Routines for full details

        """

        sequence = os.environ["WSL_SEQUENCE"]
        job_name = os.environ["WSL_JOB_NAME"]
        task_name = os.environ["WSL_TASK_NAME"]
        job_id = os.environ["WSL_JOB_KEY"]
        task_id = os.environ["WSL_TASK_KEY"]

        sql = f"""
        SET NOCOUNT ON;
        DECLARE @p_return_code nvarchar(1);
        DECLARE @p_return_msg nvarchar(1024);
        DECLARE @p_result integer;
        EXEC  Ws_Job_Release
            @p_sequence = '{sequence}'
        , @p_job_name = '{job_name}'    
        , @p_task_name = '{task_name}'
        , @p_job_id = '{job_id}'
        , @p_task_id = '{task_id}'
        , @p_release_job = '{job_name}'
        , @p_return_code = @p_return_code OUTPUT
        , @p_return_msg = @p_return_msg OUTPUT
        , @p_result = @p_result OUTPUT;
        SELECT @p_return_code AS return_code, @p_return_msg AS return_msg, @p_result AS result;"""

        try:
            crsr.execute(sql)
            results = crsr.fetchall()
            crsr.commit()

            return_code = results[0][0]
            return_msg = results[0][1]
            result = results[0][2]
        except ProgrammingError as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )
            logging.debug("A PYODBC DataError occurred:", str_ODBC_error_msg)
        return return_code, return_msg, result

    @staticmethod
    def get_extended_props(crsr, object_name: str):
        """Returns all extended property values from the repository for a given object
        as a dictionary.

        An object exists in the repository if it has a row in the ws_obj_object table.

        WhereScape's Common module makes the mistake of opening and
        closing a connection to the repository for each call"""
        extended_props = []
        sql = f"""
        SELECT  epd_variable_name as ext_key, COALESCE(tab.epv_value, src.epv_value, tgt.epv_value, '') as ext_value
        FROM ws_ext_prop_def def
        LEFT OUTER JOIN ws_ext_prop_value tab
        ON tab.epv_obj_key = ( SELECT oo_obj_key 
                                FROM ws_obj_object 
                                WHERE oo_name = '{object_name}'
                            )
        AND tab.epv_def_key = def.epd_key
        LEFT OUTER JOIN ws_ext_prop_value src
        ON src.epv_obj_key = ( SELECT lt_connect_key 
                                FROM ws_load_tab 
                                WHERE lt_table_name = '{object_name}'
                            )
        AND src.epv_def_key = def.epd_key
        LEFT OUTER JOIN ws_ext_prop_value tgt
        ON tgt.epv_obj_key = ( SELECT dc_obj_key
                                FROM ws_dbc_connect
                                JOIN ws_dbc_target
                                ON dt_connect_key = dc_obj_key
                                JOIN ws_obj_object
                                ON oo_target_key = dt_target_key
                                WHERE oo_name = '{object_name}'
                            )
        AND tgt.epv_def_key = def.epd_key
        ORDER BY epd_variable_name;
"""
        try:
            crsr.execute(sql)
            crsr_results = crsr.fetchall()
            extended_props = {row[0]: row[1] for row in crsr_results}
        except Exception as ODBC_error_msg:
            str_ODBC_error_msg = (
                str(ODBC_error_msg).replace("\\n", " ").replace("'", " ")
            )
            logging.debug("A PYODBC DataError occurred:", str_ODBC_error_msg)
        return extended_props

    @staticmethod
    def get_os_env_vars():
        """Returns all WhereScape OS environment variables
        as a dictionary of key value pairs"""
        os_env_vars = []
        try:
            os_env_vars = {
                "DEBUG": os.environ.get("DEBUG", ""),
                "WSL_BINDIR": os.environ.get("WSL_BINDIR", ""),
                "WSL_DATABASE": os.environ.get("WSL_DATABASE", ""),
                "WSL_EXP_DB": os.environ.get("WSL_EXP_DB", ""),
                "WSL_EXP_FULLNAME": os.environ.get("WSL_EXP_FULLNAME", ""),
                "WSL_EXP_NAME": os.environ.get("WSL_EXP_NAME", ""),
                "WSL_EXP_TABLE": os.environ.get("WSL_EXP_TABLE", ""),
                "WSL_EXP_SCHEMA": os.environ.get("WSL_EXP_SCHEMA", ""),
                "WSL_JOB_KEY": os.environ.get("WSL_JOB_KEY", ""),
                "WSL_JOB_NAME": os.environ.get("WSL_JOB_NAME", ""),
                "WSL_LOAD_DB": os.environ.get("WSL_LOAD_DB", ""),
                "WSL_LOAD_DB": os.environ.get("WSL_LOAD_DB", ""),
                "WSL_LOAD_FULLNAME": os.environ.get("WSL_LOAD_FULLNAME", ""),
                "WSL_LOAD_NAME": os.environ.get("WSL_LOAD_NAME", ""),
                "WSL_LOAD_SCHEMA": os.environ.get("WSL_LOAD_SCHEMA", ""),
                "WSL_LOAD_TABLE": os.environ.get("WSL_LOAD_TABLE", ""),
                "WSL_LOAD_TYPE": os.environ.get("WSL_LOAD_TYPE", ""),
                "WSL_METABASE": os.environ.get("WSL_METABASE", ""),
                "WSL_META_DB": os.environ.get("WSL_META_DB", ""),
                "WSL_META_DBID": os.environ.get("WSL_META_DBID", ""),
                "WSL_META_DSN": os.environ.get("WSL_META_DSN", ""),
                "WSL_META_DSN_ARCH": os.environ.get("WSL_META_DSN_ARCH", ""),
                "WSL_META_PWD": os.environ.get("WSL_META_PWD", ""),
                "WSL_META_SCHEMA": os.environ.get("WSL_META_SCHEMA", ""),
                "WSL_META_SERVER": os.environ.get("WSL_META_SERVER", ""),
                "WSL_META_USER": os.environ.get("WSL_META_USER", ""),
                "WSL_PWD": os.environ.get("WSL_PWD", ""),
                "WSL_SEQUENCE": os.environ.get("WSL_SEQUENCE", ""),
                "WSL_SERVER": os.environ.get("WSL_SERVER", ""),
                "WSL_SRC_DB": os.environ.get("WSL_SRC_DB", ""),
                "WSL_SRC_DBID": os.environ.get("WSL_SRC_DBID", ""),
                "WSL_SRC_DBPORT": os.environ.get("WSL_SRC_DBPORT", ""),
                "WSL_SRC_DBTYPE": os.environ.get("WSL_SRC_DBTYPE", ""),
                "WSL_SRC_DSN": os.environ.get("WSL_SRC_DSN", ""),
                "WSL_SRC_DSN_ARCH": os.environ.get("WSL_SRC_DSN_ARCH", ""),
                "WSL_SRC_PWD": os.environ.get("WSL_SRC_PWD", ""),
                "WSL_SRC_SCHEMA": os.environ.get("WSL_SRC_SCHEMA", ""),
                "WSL_SRC_SERVER": os.environ.get("WSL_SRC_SERVER", ""),
                "WSL_SRC_USER": os.environ.get("WSL_SRC_USER", ""),
                "WSL_TASK_KEY": os.environ.get("WSL_TASK_KEY", ""),
                "WSL_TASK_NAME": os.environ.get("WSL_TASK_NAME", ""),
                "WSL_TEMP_DB": os.environ.get("WSL_TEMP_DB", ""),
                "WSL_TGT_DBID": os.environ.get("WSL_TGT_DBID", ""),
                "WSL_TGT_DBPORT": os.environ.get("WSL_TGT_DBPORT", ""),
                "WSL_TGT_DSN": os.environ.get("WSL_TGT_DSN", ""),
                "WSL_TGT_DSN_ARCH": os.environ.get("WSL_TGT_DSN_ARCH", ""),
                "WSL_TGT_PWD": os.environ.get("WSL_TGT_PWD", ""),
                "WSL_TGT_SERVER": os.environ.get("WSL_TGT_SERVER", ""),
                "WSL_TGT_USER": os.environ.get("WSL_TGT_USER", ""),
                "WSL_USER": os.environ.get("WSL_USER", ""),
                "WSL_WORKDIR": os.environ.get("WSL_WORKDIR", ""),
            }
        except Exception as OS_error_msg:
            str_OS_error_msg = str(OS_error_msg).replace("\\n", " ").replace("'", " ")
            logging.debug("An OS error occurred:", str_OS_error_msg)
        return os_env_vars

    @staticmethod
    def set_proper_return_msg(ret_code: str, obj_name: str, ret_msg: str):
        """Returns a proper return message for a job run"""

        if ret_code == "1":
            return f"{ret_msg} into {obj_name} table"
        elif ret_code == "-1":
            return f"{ret_msg} to {obj_name} completed with warnings"
        elif ret_code == "-2":
            return f"Job {obj_name} failed.  Check logs due to {ret_msg}"
        elif ret_code == "-3":
            return f"Job {obj_name} experienced a fatal error due to {ret_msg}"
        elif ret_code == None:
            return f"Job {obj_name} failed.  The script failed to run.  Check the audit log for details due to {ret_msg}"
        else:
            return f"Job {obj_name} failed with return code {ret_code}.  Check the audit log for details due to {ret_msg}"

    @staticmethod
    def ExitHandler(result_code, result_message):
        try:
            wsl_job_key = os.environ["WSL_JOB_KEY"]
        except KeyError:
            wsl_job_key = "0"
        
        try:
            wsl_job_name = os.environ["WSL_JOB_NAME"]
        except KeyError:
            wsl_job_name = "Develop"

        if (
               wsl_job_key == "0"
            and wsl_job_name == "Develop"
        ):
            exit_code = 0
            print(str(result_code))
            print(result_message)
        elif result_code == 1:
            exit_code = 0
                        
            print(result_code)
            print(result_message)
        else:
            logging.debug(f"ExitHandler error occurred: {result_message}")
            exit_code = result_code
            
            print(exit_code)
            print(result_message)

        sys.exit(exit_code)
