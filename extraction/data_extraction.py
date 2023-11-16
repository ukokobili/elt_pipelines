#!/usr/bin/env python
# coding: utf-8

# # Data Source Systems
# Source data from various source systems and ingest them using python code.
# 
# Parquet files
# CSV files
# APIs
# RDBMS databases
# HTML


# import modules
import certifi
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import urllib3
from urllib3 import request
from unicodedata import normalize
import logging
from config import log_config  # Ensure that log_config is imported

# Configure logging using the log_config function from config.py
log_config()

# Parquet & CSV files Data Sources
# curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet
# curl -O https://data.cityofnewyork.us/resource/h9gi-nx95.csv?$limit=500

# define top level module logger
logger = logging.getLogger(__name__)

# Sourcing Parquet data
# Read data from the Parquet file. We use pandas read_parquet method for ease and speed.
def parquet_data_source(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        logger.info( f'{parquet_file_name} : extracted {df_parquet.shape[0]} records from the parquet file' )
    except Exception as e:
        logger.exception( f'{parquet_file_name} : - exception {e} encountered while extracting the parquet file' )
        df_parquet = pd.DataFrame()
    return df_parquet


# Sourcing CSV data
# Read data from the CSV file. We use pandas read_csv method for ease and speed.
def csv_data_source(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
        logger.info( f'{csv_file_name} : extracted {df_csv.shape[0]} records from the csv file' )
    except Exception as e:
        logger.exception( f'{csv_file_name} : -exception {e} encountered while extracting the csv file' )
        df_csv = pd.DataFrame()
    return df_csv


# Sourcing Data from RDBMS tables
# # Read sqlite query results into a pandas DataFrame
# with sqlite3.connect("movies.sqlite") as conn:
#     df = pd.read_sql("select * from movies", conn)
# df.head()


# Sourcing data from APIs
# Please make sure to install the certifi library 
# URL for the API

def api_data_source(api_endpoint):
    try:
        # Create a Pool manager that can be used to read the API response 
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
        
        # Check if API is available to retrieve the data
        apt_status = http.request('GET', api_endpoint).status
        
        if apt_status == 200:
            logger.info( f'{apt_status} - ok : while invoking the {api_endpoint}' )
            # Retrieve API data
            response = http.request('GET', api_endpoint)
            data = json.loads(response.data.decode('utf-8'))
            
            # Normalize the data into a DataFrame
            df_api = pd.json_normalize(data)
            logger.info(f'{apt_status}- extracted {df_api.shape[0]} records from the api endpoint')
        else:
            logger.error( f'{apt_status}- error : while invoking the {api_endpoint}' )
            df_api = pd.DataFrame()
    except Exception as e:
        logger.exception( f'{apt_status} : - exception {e} encountered when reading from the api')
        df_api = pd.DataFrame()
    return df_api


# Sourcing data from Webpages
# Please visit the url https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)
# get data from url

def web_data_source(web_url, matching_keyword):
    try:
        df_html = pd.read_html(web_url, match = matching_keyword)
        df_html = df_html[0]
        logger.info( f'{web_url}- read {df_html.shape[0]} records from the page : {web_url}' )
    except Exception as e:
        logger.exception(f'{web_url} : - exception {e} encountered while reading data from the page: {web_url}')
        df_html = pd.DataFrame()
    return df_html


# Data Extraction From All Sources
def data_extraction():
    parquet_file_name = '/home/nerd/elt_pipelines/data/yellow_tripdata_2022-01.parquet'
    csv_file_name = '/home/nerd/elt_pipelines/data/h9gi-nx95.csv'
    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500'
    web_url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
    matching_keyword = 'by country'

    df_parquet, df_csv, df_api, df_html = (parquet_data_source(parquet_file_name),
                                           csv_data_source(csv_file_name),
                                           api_data_source(api_endpoint),
                                           web_data_source(web_url, matching_keyword))

    return df_parquet, df_csv, df_api, df_html

