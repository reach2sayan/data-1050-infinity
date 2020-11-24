import pandasdmx as sdmx
import pymongo
from pymongo import monitoring, MongoClient, ASCENDING, DESCENDING
import pandas as pd
import logging
from log_tools import CommandLogger
import json
import log_tools
import time
from datetime import datetime, timedelta
from timeloop import Timeloop
import os
import sys
import traceback

MONGO_URI = 'mongodb+srv://sayan:infinity@infinity.9hew3.mongodb.net/<dbname>?retryWrites=true&w=majority'
#MONGO_URI = 'localhost:27017'
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
    
    #if os.path.exists(UNFCC_FILE): 
    #    os.rename(UNFCC_FILE,'old_'+UNFCC_FILE)
    #if os.path.exists(EBAL_FILE):
    #    os.rename(EBAL_FILE,'old_'+EBAL_FILE)

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
        logger.info('UNFCC Greenhouse Data stored as {}'.format(UNFCC_FILE))

        df_ebal.reset_index().to_csv(EBAL_FILE,index=False)
        logger.info('UN Energy Balance Data stored as {}'.format(EBAL_FILE))
    except Exception as e:
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

        df_ebal = pd.read_csv(EBAL_FILE)
        df_unfcc = pd.read_csv(UNFCC_FILE)
        data_json_unfcc = json.loads(df_unfcc.to_json(orient='records'))
        data_json_ebal = json.loads(df_ebal.to_json(orient='records'))

        result = coll_ebal.insert_many(data_json_ebal)
        logger.info('Inserted a total of {} records in EBAL'.format(len(result.inserted_ids)))
        result = coll_unfcc.insert_many(data_json_unfcc)
        logger.info('Inserted a total of {} records in UNFCC'.format(len(result.inserted_ids)))

    except pymongo.errors.ConnectionFailure as e:
        logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
        traceback.print_exc(file = sys.stdout)

    finally:
        client.close()

def create_index():
    """
    Indexed on the country and time period
    """
    try:
        client = MongoClient(MONGO_URI,event_listeners=[CommandLogger()])
        db = client.get_database('UNSD')
        
        coll_unfcc = db.get_collection('unfcc')
        coll_ebal = db.get_collection('ebal')
        result_unfcc = coll_unfcc.create_index([('REF_AREA',ASCENDING),('TIME_PERIOD',DESCENDING)])
        result_ebal = coll_ebal.create_index([('REF_AREA',ASCENDING),('TIME_PERIOD',DESCENDING)])
    except pymongo.errors.ConnectionFailure as e:
        logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
        traceback.print_exc(file = sys.stdout)

def update_database(df,label):
    """
    Updates the database based on the latest download
    A bit inefficient as it basically recreates the collections from scratch
    However updates are once a year. So not a big deal.
    """
    try:
        client = MongoClient(MONGO_URI,event_listeners=[CommandLogger()])
        db = client.get_database('UNSD')

        if label == 'unfcc':
            coll = db.get_collection('unfcc')
            logger.info('Getting UNFCC data from monogoDB')
        else:
            coll = db.get_collection('ebal')
            logger.info('Getting Energy Balance data from monogoDB')

        logger.info('Starting Update...')
        for record in json.loads(df.to_json(orient='records')):
            try:
                result = coll.replace_one(filter=record, # locate the document if exists
                                      replacement=record,  # latest document
                                      upsert=True)         # update if exists, insert if not
                if result.raw_result['updatedExisting']:
                    logger.info('Update existing record')
                else:
                    logger.info('Added new record: {}'.format(result.upserted_id))
            except pymongo.errors.ConnectionFailure as e:
                logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
                traceback.print_exc(file = sys.stdout)
        #data_json = json.loads(df.to_json(orient='records'))
        #result = coll.insert_many(data_json)
        #logger.info('Inserted a total of {} records in UNFCC'.format(len(result.inserted_ids)))

    except pymongo.errors.ConnectionFailure  as e:
        logger.error("Exception seen: " + str(e))
        traceback.print_exc(file = sys.stdout)
         
    finally:
        client.close()


""" 
The Update method is a bit stretch :(
Maybe in the future we will only get and put data for the latest year and ignore past updates (which are definitely sketchy)
"""
def get_updated_records(label):

    if label == 'unfcc':
        old_df = pd.read_csv('old_'+UNFCC_FILE)
        new_df = pd.read_csv(UNFCC_FILE)
    else:
        old_df = pd.read_csv('old_'+EBAL_FILE)
        new_df = pd.read_csv(EBAL_FILE)
    #update_df = new_df[~new_df.apply(tuple,1).isin(old_df.apply(tuple,1))]
    #update_df = pd.concat([old_df,new_df]).drop_duplicates(keep=False)
    update_df = new_df
    update_database(update_df,label)


t1 = Timeloop()

@t1.job(interval=timedelta(seconds=60))
def _worker():
    """
    Main work loop repeats every hour
    """
    try:
        logger.info('Looping...')
        temp_list = []
        for file in ['data_unfcc.csv','data_ebal.csv']:
            temp_list.append(os.path.isfile(file))
        if not all(temp_list):
            print('Starting from scratch...')
            download_source()
            create_database()
            create_index()

        time_mod = datetime.strptime(time.ctime(os.stat('data_ebal.csv').st_mtime),'%a %b %d %H:%M:%S %Y')
        time_now = datetime.now()

        if (time_now - time_mod).seconds > 3600:
            download_source()
            get_updated_records('unfcc')
            get_updated_records('ebal')
            create_index()
    except Exception as e:
        logger.warning('Main Loop error')

if __name__ == "__main__":
    t1.start(block=True)





