#!/usr/bin/env python
"""
This is the database side code. It creates a new database if no CSV datafiles are found
Or else updates them if the old file is older than a year.
"""
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
import urllib
from xml.etree import ElementTree
import country_converter as coco

__author__ = "Benkuang Xiong, Guansu (Frances) Niu, Sayan Samanta, and Wanxin (Christina) Ye"
__license__ = "GPL"
__maintainer__ = "Sayan Samanta"
__email__ = "sayan_samanta@brown.edu"
__status__ = "Production"


#MONGO_URI = 'mongodb+srv://sayan:infinity@infinity.9hew3.mongodb.net/<dbname>?retryWrites=true&w=majority'
MONGO_URI = 'localhost:27017'
LOG_FILE = '../../logs/infinity.log'
EBAL_FILE = '../../data_files/data_ebal.csv'
UNFCC_FILE = '../../data_files/data_unfcc.csv'

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
        df_ebal = decoding_codes(df_ebal)

        coco_dict = {}
        for i in df_ebal["REF_AREA"].unique():
            #     if i not in coco_dict:
                coco_dict[i] = coco.convert(i, to='iso3')
                coco_dict["France-Monaco"] = coco.convert("France", to='iso3')
                coco_dict["Italy-San Marino"] = coco.convert("Italy", to='iso3')
                coco_dict["Switzerland-Liechtenstein"] = coco.convert("Switzerland", to='iso3')
        df_ebal["REF_AREA"] = [coco_dict[i] for i in df_ebal["REF_AREA"]]

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

def decoding_codes(df_ebal):
    """
    Replaces the dataflow codes to human readable format
    """
    f = urllib.request.urlopen("https://data.un.org/ws/rest/codelist/unsd/CL_AREA/").read()
    root = ElementTree.fromstring(f)
    area_dict = {}
    for ele in root[1][0][0][2:]:
        try:
            area_dict[ele.attrib["id"]] = ele[0].text
        except AttributeError:
            continue

    f = urllib.request.urlopen("https://data.un.org/ws/rest/codelist/unsd/CL_COMMODITY_ENERGY_BALANCE_UNDATA/").read()
    root = ElementTree.fromstring(f)
    comm_dict = {}
    for ele in root[1][0][0][1:]:
        try:
            comm_dict[ele.attrib["id"]] = ele[0].text
        except AttributeError:
            continue

    f = urllib.request.urlopen("https://data.un.org/ws/rest/codelist/unsd/CL_TRANS_ENERGY_BALANCE_UNDATA/").read()
    root = ElementTree.fromstring(f)
    trans_dict = {}
    for ele in root[1][0][0][1:]:
        try:
            trans_dict[ele.attrib["id"]] = ele[0].text
        except AttributeError:
            continue

    df_ebal["COMMODITY"] = [comm_dict[i] for i in df_ebal["COMMODITY"].values]
    df_ebal["TRANSACTION"] = [trans_dict[i] for i in df_ebal["TRANSACTION"].values]
    df_ebal["REF_AREA"] = [area_dict["{:03d}".format(i)] for i in df_ebal["REF_AREA"].values]


    return df_ebal

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
            coll.delete_many({})
            logger.info('Getting UNFCC data from mongoDB')
        else:
            coll = db.get_collection('ebal')
            coll.delete_many({})
            logger.info('Getting Energy Balance data from mongoDB')

        logger.info('Starting Update...')

        data_json = json.loads(df.to_json(orient='records'))

        coll.insert_many(data_json)
        #for record in json.loads(df.to_json(orient='records')):
        #    try:
        #        result = coll.replace_one(filter=record, # locate the document if exists
        #                              replacement=record,  # latest document
        #                              upsert=True)         # update if exists, insert if not
        #        if result.raw_result['updatedExisting']:
        #            logger.info('Update existing record')
        #        else:
        #            logger.info('Added new record: {}'.format(result.upserted_id))
        #    except pymongo.errors.ConnectionFailure as e:
        #        logger.error('PyMongo error ConnectionFailure seen: ' + str(e))
        #        traceback.print_exc(file = sys.stdout)
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
        #old_df = pd.read_csv('old_'+UNFCC_FILE)
        df = pd.read_csv(UNFCC_FILE)
    else:
        #old_df = pd.read_csv('old_'+EBAL_FILE)
        df = pd.read_csv(EBAL_FILE)
        df = decoding_codes(df)
        coco_dict = {}
        for i in df["REF_AREA"].unique():
            #     if i not in coco_dict:
            coco_dict[i] = coco.convert(i, to='iso3')
            coco_dict["France-Monaco"] = coco.convert("France", to='iso3')
            coco_dict["Italy-San Marino"] = coco.convert("Italy", to='iso3')
            coco_dict["Switzerland-Liechtenstein"] = coco.convert("Switzerland", to='iso3')
        df["REF_AREA"] = [coco_dict[i] for i in df["REF_AREA"]]
    #update_df = new_df[~new_df.apply(tuple,1).isin(old_df.apply(tuple,1))]
    #update_df = pd.concat([old_df,new_df]).drop_duplicates(keep=False)
    update_database(df,label)


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
#    t1.start(block=True)
    create_database()
#    get_updated_records('unfcc')
#    get_updated_records('ebal')



