from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
from fm13_ingest.decode_fm13 import decode
import arrow
import os
from dotenv import load_dotenv

'''

This file takes a JSON document and inserts it into a Postgres database

'''
DB_STRING = "postgres://postgres:password@localhost:5432/postgis"

TABLE_NAME = 'meds_buoys_realtime_non_opp'


class DBIngester(object):
    def __init__(self):

        self.TABLE_NAME = TABLE_NAME
        # setup DB
        self.db = create_engine(DB_STRING)
        table_meta = MetaData(self.db)
        table = Table(TABLE_NAME, table_meta, autoload=True)
        self.table_columns = table.columns
        self.table = table

    def insert_into_db(self, line: dict) -> dict:
        'inserts dictionary into DB'

        # load existing table columns list

        insertstmt = insert(self.table).values(line)

        stmt = insertstmt.on_conflict_do_update(
            index_elements=[self.table.c.station_id, self.table.c.result_time],
            set_=line
        )

        return self.db.execute(stmt)

    def remove_and_log_uninsertable_keys(self, line):
        '''Remove and report any fields that are in the XML but arent in the database
        This shouldnt be needed, but just in case a new field is added at the source before
        we have a chance to add the column to the DB
        '''

        for k in list(line.keys()):
            if k not in self.table_columns:
                print(f"Cannot insert {k}")
                del line[k]

    def ingest(self, filename: str, metadata={}):
        ''' Ingest FM13 file to database. Also gets some metadata from AMPQ
        '''
        with open(filename) as f:
            lines = f.read()
            record = decode(lines)

            # sundew_extension just contains some extra metadata
            if 'sundew_extension' in metadata:
                splat = metadata['sundew_extension'].split(":")
                office = splat[1]
                dateRaw = splat[5]
                dateStr = str(arrow.get(dateRaw, "YYYYMMDDHHmmss"))

                record['result_time'] = dateStr
                record['headers'] = metadata
                record['headers']['sundew'] = splat
                record['office'] = office

            self.remove_and_log_uninsertable_keys(record)
            res = self.insert_into_db(record)

            if res:
                print(
                    f'{res.rowcount} rows update or inserted in table "{TABLE_NAME}"')
                return res.rowcount
            return None
