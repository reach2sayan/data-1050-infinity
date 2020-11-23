import pandasdmx as sdmx
from pymongo import monitoring, MongoClient
import pandas as pd
import logging
import json
import log_tools
import time
import os
import sys
import traceback

MONGO_URI = 'mongodb+srv://sayan:infinity@infinity.9hew3.mongodb.net/<dbname>?retryWrites=true&w=majority'
LOG_FILE = 'infinity.log'
EBAL_FILE = 'data_ebal.csv'
UNFCC_FILE = 'data_unfcc.csv'

logger = logging.Logger(__name__)
log_tools.setup_log(logger, LOG_FILE)

monitoring.register(CommandLogger())

def download_source():
    """
    Downloads the following datastructure from the UN SDMX API
    (1) United Nations Energy Balance Data
    (2) United Nations Greenhouse gas Emission Data
    Data is stored at corresponding CSV files
    """
    
    if os.path.exists(UNFCC_FILE): 
        os.rename(UNFCC_CILE,'old_'+UNFCC_FILE)
    if os.path.exists(EBAL_FILE):
        os.rename(EBAL_FILE,'old_'+EBAL_FILE)

    try:
        unsd = sdmx.Request('UNSD')
        sdmx.logger.setLevel(logging.INFO)
        
        logger.info('Loading UNFCC Data')
        resp_unfcc = unsd.data('DF_UNData_UNFCC')

        logger.info('Loading UN Energy Balance Data')
        resp_ebal = unsd.data('DF_UNData_EnergyBalance')
    except Exception as e:
        logger.error('Error!! Please look at SDMX logs to troubleshoot' + str(e))
        traceback.print_exc(file = sys.stdout)

    try:
        df_ebal = resp_ebal.to_pandas()
        df_unfcc = resp_unfcc.to_pandas()

        df_unfcc.reset_index().to_csv(UNFCC_FILE,index=False)
        logger.error('UNFCC Greenhouse Data stored as data_unfcc.csv')

        df_ebal.reset_index().to_csv(EBAL_FILE,index=False)
        logger.error('UN Energy Balance Data stored as data_unebal.csv')
    except Exception e:
        logger.error('Error!! While saving data from SDMX to CSV ' + str(e))
        traceback.print_exc(file = sys.stdout)

def create_database():
    """
    Creates a new mongoDB database from scratch. Executed only when the mongoDB server is empty 
    """

    try:
        client = MongoClient(MONGO_URI,event_listeners=[CommandLogger()])
        db = client.get_database('UNSD')

        coll_ebal = db.get_collection('ebal')
        coll_unfcc = db.get_collection('unfcc')

        df_ebal = pd.read_csv('data_ebal.csv')
        df_unfcc = pd.read_csv('data_unfcc.csv')
        data_json_unfcc = json.loads(df_unfcc.to_json(orient='records'))
        data_json_ebal = json.loads(df_ebal.to_json(orient='records'))

        col_ebal.insert_many(data_json_ebal)
        col_unfcc.insert_many(data_json_unfcc)

    except pymongo.errors.ConnectionFailure as e:
        logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
        traceback.print_exc(file = sys.stdout)

    finally:
        client.close()

def update_database_unfcc(df,label):
    """
    Some docstring
    """

    try:
        match_count = 0
        mod_count= 0
        upserted_ids = []
        client = MongoClient(MONGO_URI,event_listeners=[CommandLogger()])
        db = client.get_database('UNSD')

        if label == 'unfcc':
            coll = db.get_collection('unfcc')
        else:
            coll = db.get_collection('ebal')

        for record in df.to_dict('records'):
            try:
                result = coll.replace_one(filter={}, # locate the document if exists
                                      replacement=record,  # latest document
                                      upsert=True)         # update if exists, insert if not
                if result.matched_count > 0:
                    match_count += 1
                if result.modified_count > 0:
                    mod.count += 1
                    upserted_ids.append(result.upserted_id)
            except pymongo.errors.ConnectionFailure as e:
                logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
                traceback.print_exc(file = sys.stdout)
    except Exception as e:
        logger.error("Exception seen: " + str(e))
        traceback.print_exc(file = sys.stdout)
        
        logger.info('Matched Docs: {}, Modified Docs: {}'.format(match_count,mod_count))
    finally:
        client.close()

