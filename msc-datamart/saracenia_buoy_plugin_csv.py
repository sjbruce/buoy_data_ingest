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

        from msc_ingest.ingest_to_csv import ingest_buoy_xml_file

        new_file = parent.msg.new_file
        parent.logger.info('Buoy ingest file: ' + new_file)
        ingest_buoy_xml_file(new_file)

        return True


csv_ingest = CSV_ingest(self)

self.on_file = csv_ingest.perform
