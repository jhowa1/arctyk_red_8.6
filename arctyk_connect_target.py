""" arctyk_connect_target.py     Author: Jack W. Howard   Wellspring Holdings, LLC   Date: 2022-09-15
This software is the property of Wellspring Holdings, LLC and is not to be disclosed, copied, or shared
without the express written consent of Wellspring Holdings, LLC.

This module manages the connection to the target database.   It parses sql statement results for use in
in the WhereScape Scheduler. 

A bucket store is also defined as a target for parquet files.

Functions:
    crt_cnxn: create a connection to the target database
    crt_cursor: create a cursor using the connection function to the target database

"""
import logging
import snowflake.connector as Connector

logger = logging.getLogger(__name__)

class BucketStore:
    def __init__(self, bucket, base_file_key, max_parts_in_upload):
        self.bucket = bucket
        self.base_file_key = base_file_key
        self.max_parts_in_upload = max_parts_in_upload

def crt_cnxn():
    """Create connection to the SnowFlake database"""
    cnxn = Connector.connect(
        user="jack",
        password="1081Lakeshore",
        account="XBA39423",
        database="DB",
        warehouse="COMPUTE_WH",
        schema="LOAD",
        role="SYSADMIN",
    )
    
    return cnxn

def crt_cursor():
    """Create connection & cursor to the SnowFlake database"""
    cnxn = crt_cnxn()
    crsr = cnxn.cursor()
    return crsr



    target_store = BucketStore(
        bucket="wellspringholdings",
        base_file_key="wsl/test_result_2015.par",
        max_parts_in_upload=10
    )