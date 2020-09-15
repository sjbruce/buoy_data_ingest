#!/usr/bin/python3

"""
    Basic Sarracenia plugin to load the buoy data ingestion script

    See https://github.com/MetPX/sarracenia/blob/master/doc/Prog.rst

"""


class CSV_ingest(object):

    def __init__(self, parent):
        parent.logger.debug("Buoy plugin for CSV conversion initialized")

    def perform(self, parent):
        '''Imports have to be here..
        See https://github.com/MetPX/sarracenia/blob/master/doc/Prog.rst#why-doesnt-import-work

        '''
        import os
        import msc_ingest.ingest_to_csv as ci
        import configparser

        logger = parent.logger
        
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(ci.__file__), 'ingest_to_csv.conf')
        logger.info("Configuration Path: %s" % (config_path))
        result = config.read(config_path)
        logger.info("Configuration Loading Result: %s" % (result))

        new_file = os.path.join(parent.msg.new_dir, parent.msg.new_file)
        logger.info('Buoy ingest file: ' + new_file)
        ci.process_file(new_file, config=config, logger=logger)

        return True


csv_ingest = CSV_ingest(self)

self.on_file = csv_ingest.perform
