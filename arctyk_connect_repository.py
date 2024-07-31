""" arctyk_connect_repository.py Author: Jack W. Howard   Wellspring Holdings, LLC   Date: 2022-09-15
This software is the property of Wellspring Holdings, LLC and is not to be disclosed, copied, 
or shared without the express written consent of Wellspring Holdings, LLC.

This module manages the repository database connection.  It is separated from other connection
modules to enable the use of alternative authentication methods.

Methods:
    crt_cursor: create a cursor to the repository database

"""
import logging
import json
import pyodbc
import yaml


logger = logging.getLogger(__name__)

@staticmethod
def crt_cursor():
    """Connect to the repository and return a cursor"""
    try:
        with open(
            "C:/ProgramData/WhereScape/Modules/WslPython/arctyk_config.yaml", "r"
            , encoding="utf-8"
        ) as yaml_file:
            config = yaml.safe_load(yaml_file)

        db_objects = config["database_objects"]
        meta_dsn = db_objects["meta_dsn"]
        repo_cnxn = pyodbc.connect(DSN=meta_dsn)
        repo_crsr = repo_cnxn.cursor()
    except pyodbc.Error as err_message:
        logging.debug("Failed to connect to the meadata database: %s", err_message)
    else:
        return repo_crsr

@staticmethod
def get_source_DSN(source_connection_name:str):
    """Get the DSN for a SQL Server source for BCP"""
    try:
        dsn_sql = f"SELECT [dc_odbc_source] FROM [wsh].[dbo].[ws_dbc_connect] WHERE [dc_name] = '{source_connection_name}'"
        repo_crsr = crt_cursor()
        results = repo_crsr.execute(dsn_sql).fetchone()        
        return results[0]
    except pyodbc.Error as err_message:
        logging.debug("Failed to fetch DSN from source_connection: %s: %s", source_connection_name, err_message)

@staticmethod
def get_source_conn_str(source_connection_name:str):
    """Get the connection string for a source connection"""
    try:
        dsn_sql = f"""SELECT
  [oat_att_values_json]
FROM [wsh].[dbo].[ws_object_attributes] join
  [dbo].[ws_obj_object] on oat_obj_key = oat_obj_key
WHERE 
  oo_name = '{source_connection_name}' and
  oat_att_values_json like '%connectionString%' """
        repo_crsr = crt_cursor()
        results = repo_crsr.execute(dsn_sql).fetchone()        
        json_data = results[0]
        config = json.loads(json_data)

        # Extract and process configuration fields
        for field in config['uiConfigFields']:
            name = field['name']
            value = field['value']
            set_env_var = field['setEnvVarForScripts']

            # Set the global variable with the name from the JSON
            globals()[name] = value

        return globals()['connectionString']

    except pyodbc.Error as err_message:
        logging.debug("Failed to fetch DSN from source_connection: %s: %s", source_connection_name, err_message)