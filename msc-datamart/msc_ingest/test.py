import os.path
import msc_ingest.ingest_to_csv as csv_import

test_source_file = 'H:/Temp/msc-datamart/temp/atlantic/2020-09-08-1530-4400488-AUTO-swob.xml'

csv_import.ingest_buoy_xml_file(test_source_file)
