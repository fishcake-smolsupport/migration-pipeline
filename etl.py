#! /usr/bin/env python3

from src.db import DatabaseConnector
from src.log import logger
from dotenv import dotenv_values
from sqlalchemy import select, and_, func
import logging
import os
import pandas as pd

CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ
    
@logger
def extract_postpaid():

    querypath = '/home/yamweng/migration-pipeline/scripts/postpaid_portion.sql'
    production_env = DatabaseConnector(CONFIG['PRD_KEY'])
    production_env.setup_engine()
    
    with open(querypath, 'r') as file:
        sql_statement = file.read()

    results = production_env.execute_query(sql_statement)

    if results:
        column_names = results.keys()  # Accessing column names
        rows = results.fetchall()      # Fetch all rows as list of tuples
        df = pd.DataFrame(rows, columns=column_names)
    else:
        df = pd.DataFrame()  # Return an empty DataFrame if no results

    print(df.shape)
    print(df.head())
    return df

@logger
def extract_prepaid():

    querypath = '/home/yamweng/migration-pipeline/scripts/prepaid_portion.sql'
    production_env = DatabaseConnector(CONFIG['PRD_KEY'])
    production_env.setup_engine()
    
    with open(querypath, 'r') as file:
        sql_statement = file.read()

    results = production_env.execute_query(sql_statement)

    if results:
        column_names = results.keys()  # Accessing column names
        rows = results.fetchall()      # Fetch all rows as list of tuples
        df = pd.DataFrame(rows, columns=column_names)
    else:
        df = pd.DataFrame()  # Return an empty DataFrame if no results

    print(df.shape)
    print(df.head())
    return df

@logger
def load_data(df: pd.DataFrame, table_name: str, schema_name: str = 'dbo'):
    reporting_env = DatabaseConnector(CONFIG['INC_KEY'])
    reporting_env.setup_engine()
    
    df.to_sql(table_name, reporting_env.engine, if_exists='replace', index=False, schema=schema_name)
    print(f'Saved dataframe to {schema_name}.{table_name}')


if __name__ == "__main__" :
    postpaid_df = extract_postpaid() 
    prepaid_df = extract_prepaid()

    load_data(postpaid_df, 'simulator_based_on_incentive_postpaid_temp')
    load_data(prepaid_df, 'simulator_based_on_incentive_prepaid_temp')