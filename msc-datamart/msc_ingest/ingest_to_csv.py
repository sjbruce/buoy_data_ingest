'''
Script that is run by a sarracenia plugin when new buoy data is received, 
inserts parsed XML into flat files for ingestion by ERDDAP

See docs at https://dd4.weather.gc.ca/observations/doc/
'''

import os
import click
import pandas as pd
from datetime import datetime
import dateutil
from msc_ingest.parse_xml import buoy_xml_to_json

import configparser
import logging

# Logging & Configuration Defaults
default_log_file = os.path.join(os.path.dirname(__file__), 'ingest_to_csv.log')
default_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s - @ line %(lineno)d in %(pathname)s'
default_log_level = logging.DEBUG

default_config_file = os.path.join(os.path.dirname(__file__), 'ingest_to_csv.conf')

logging.basicConfig(filename=default_log_file, format=default_log_format, level=default_log_level)
logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read(default_config_file)

def insert_into_csv(line, config=config, logger=logger):
    '''
    inserts dictionary into csv file
    '''
    output_dir = config['DEFAULT']['output_dir']
    index_field = config['DEFAULT']['index_field']
    check_duplicates_field = config['DEFAULT']['check_duplicates_field']
    station_id_field = config['DEFAULT']['station_id_field']
    dt_format = config['DEFAULT']['datetime_format']
    filename_format = config['DEFAULT']['filename_format']

    # sampling_time = datetime.strptime(line[index_field], '%Y-%m-%dT%H:%M:%S.%fZ')
    sampling_time = dateutil.parser.isoparse(line[index_field])

    filename = filename_format % (line[station_id_field], sampling_time.strftime(dt_format))
    full_path = os.path.join(output_dir, filename)

    # converting the dictionary values to be a single element array allows 
    # pandas to better identify how to interpret the data
    converted_line = prepare_line(line)
    
    line_df = pd.DataFrame.from_dict(converted_line)

    if os.path.isfile(full_path):
        existing_data = pd.read_csv(full_path)

        # append new line data to dataframe and purge duplciates
        buoy_data = existing_data.append(line_df)
        buoy_data.drop_duplicates(subset=check_duplicates_field, inplace=True)

        # convert sampling_time to a datetime field and then set as index
        buoy_data[index_field] = pd.to_datetime(buoy_data[index_field])
        buoy_data.set_index(index_field, inplace=True)

        buoy_data.sort_index(inplace=True)
    else:
        line_df[index_field] = pd.to_datetime(line_df[index_field])
        line_df.set_index(index_field, inplace=True)
        buoy_data = line_df

    logger.debug("# of records in DataFrame: %s, File: %s" % (len(buoy_data.index), full_path))
    return buoy_data, full_path

def prepare_line(line):
    '''
    Converts line data to a form Pandas will recognize as a row of data rather 
    than a series
    https://eulertech.wordpress.com/2017/11/28/pandas-valueerror-if-using-all-scalar-values-you-must-pass-an-index/
    '''
    converted_line = {}

    for key, value in line.items():
        converted_line[key] = [value]

    return converted_line


def remove_and_log_uninsertable_keys(line):
    '''Remove and report any fields that are in the XML but arent in the database
       This shouldnt be needed, but just in case a new field is added at the source before
       we have a chance to add the column to the DB
    '''
    column_headers = config['DEFAULT']['column_headers'].split(',')

    for k in list(line.keys()):
        if k not in column_headers:
            logger.warning("Cannot insert: %s" % (k))
            del line[k]


def read_buoys_list(filename, logger):
    logger.debug("Looking for file: %s, Is File?: %s" % (filename, os.path.isfile(filename)))

    file = open(filename)
    buoys_list = file.read().strip().split('\n')
    logger.debug("Buoy List: %s" % (buoys_list))
    file.close()

    return buoys_list


def ingest_buoy_xml_file(filename, config=config, logger=logger):
    '''
    Insert buoy data in XML format into a Pandas DataFrame to be written out to
    a CSV file
    '''
    logger.debug("Source File: %s" % (filename))
    
    try:
        buoy_filename = config['DEFAULT']['buoy_list']
        station_id_field = config['DEFAULT']['station_id_field']
    except:
        buoy_filename = None
        logger.error("Current Working Directory: %s, file: %s" % (os.getcwd(), filename))

    this_dir = os.path.dirname(os.path.realpath(__file__))

    buoys_list = []

    # checks absolute path first
    if os.path.isfile(buoy_filename):
        buoys_list = read_buoys_list(buoy_filename, logger)

    # falls back to relative path next
    elif os.path.isfile(os.path.join(this_dir, buoy_filename)):
        buoys_list = read_buoys_list(os.path.join(this_dir, buoy_filename), logger)

    else:
        logger.error('Cannot find buoy file at path: %s or in directory: %s!' % (buoy_filename, this_dir))

    if buoys_list:
        # get a flat dictionary from the source XML
        buoy_record = buoy_xml_to_json(filename)
        logger.debug("JSON Data: %s" % (buoy_record))

        if buoy_record[station_id_field] not in buoys_list:
            logger.error("Buoy %s not in: %s" % (buoy_record[station_id_field], ' '.join(buoys_list)))

            # An empty dataframe is what's expected if 
            # there is no data to write out
            return pd.DataFrame()

        # remove and print extra fields that our table doesn't have (yet)
        # probably not necessary if we keep an eye on the mailing lists
        remove_and_log_uninsertable_keys(buoy_record)

        # insert the record
        logger.debug("Running insert_into_csv()...")
        return insert_into_csv(buoy_record, config, logger)
    else:
        return pd.DataFrame()

def process_file(filename, config=config, logger=logger):
    buoy_data, full_path = ingest_buoy_xml_file(filename, config, logger)

    logger.debug('DataFrame to Write has %s rows' % (len(buoy_data.index)))
    if not buoy_data.empty:
        logger.debug("Preparing to write dataframe to file: %s..." % (full_path))
        buoy_data.to_csv(path_or_buf=full_path, date_format='%Y-%m-%dT%H:%M:%SZ')
        logger.info('%s rows update or inserted in directory' % (len(buoy_data.index)))
    else:
        logger.debug("buoy_data is empty??!")


@click.command()
@click.argument('source_path', required=True, type=click.Path(exists=True))
@click.option('--bulk', is_flag=True)
@click.option('--config_file', default=default_config_file, show_default=True, type=click.Path(exists=True))
@click.option('--log', default=default_log_file, show_default=True, type=click.Path())
def main(source_path, bulk, config_file, log):
    logger.debug('Source Path: %s' % (source_path))
    logger.debug('Bulk Flag: %s' % (bulk))
    logger.debug('Config File: %s' % (config_file))

    if not bulk:
        process_file(source_path, config, logger)
    else:
        source_directory = os.fsencode(source_path)
            
        for file in os.listdir(source_directory):
            swob_xml = os.path.join(source_path, os.fsdecode(file))
            process_file(swob_xml, config, logger)

if __name__ == '__main__':
    main()
