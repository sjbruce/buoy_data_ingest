'''

Script that is run by a sarracenia plugin when new buoy data is received, inserts parsed XML into postgres database table `swob_marine`


See docs at https://dd4.weather.gc.ca/observations/doc/

'''

# TODO add logging
# TODO could auto-create table here

import os
import click
import pandas as pd
from msc_ingest.parse_xml import buoy_xml_to_json

# TODO: Move these values/definitions to an external configuration file
buoy_filename = "buoy_list.txt"
output_dir = 'H:/Temp/msc-datamart/data/'
column_headers = [
    'sampling_time',
    'result_time',
    'stn_typ',
    'wmo_synop_id',
    'wmo_identifier_extended',
    'stn_nam',
    'msc_id',
    'stn_elev',
    'sensor_table_number',
    'lat',
    'long',
    'date_tm',
    'buoy_typ',
    'rpt_typ',
    'crnt_buoy_lat',
    'crnt_buoy_lat_qa_summary',
    'crnt_buoy_lat_data_flag',
    'crnt_buoy_long',
    'crnt_buoy_long_qa_summary',
    'crnt_buoy_long_data_flag',
    'avg_crnt_volt_pst10mts',
    'avg_crnt_volt_pst10mts_1',
    'avg_solr_panl_crnt_pst10mts',
    'avg_solr_panl_crnt_pst10mts_qa_summary',
    'avg_solr_panl_crnt_pst10mts_data_flag',
    'avg_solr_panl_crnt_pst10mts_1',
    'avg_solr_panl_crnt_pst10mts_1_qa_summary',
    'avg_solr_panl_crnt_pst10mts_1_data_flag',
    'avg_batry_volt_pst10mts',
    'avg_batry_volt_pst10mts_qa_summary',
    'avg_batry_volt_pst10mts_data_flag',
    'avg_batry_volt_pst10mts_1',
    'avg_batry_volt_pst10mts_1_qa_summary',
    'avg_batry_volt_pst10mts_1_data_flag',
    'avg_air_temp_pst10mts',
    'avg_air_temp_pst10mts_qa_summary',
    'avg_air_temp_pst10mts_data_flag',
    'avg_air_temp_pst10mts_1',
    'avg_air_temp_pst10mts_1_qa_summary',
    'avg_air_temp_pst10mts_1_data_flag',
    'avg_stn_pres_pst10mts',
    'avg_stn_pres_pst10mts_qa_summary',
    'avg_stn_pres_pst10mts_data_flag',
    'avg_stn_pres_pst10mts_1',
    'avg_stn_pres_pst10mts_1_qa_summary',
    'avg_stn_pres_pst10mts_1_data_flag',
    'avg_stn_pres_pst10mts_2',
    'avg_stn_pres_pst10mts_2_qa_summary',
    'avg_stn_pres_pst10mts_2_data_flag',
    'avg_sea_sfc_temp_pst10mts',
    'avg_sea_sfc_temp_pst10mts_qa_summary',
    'avg_sea_sfc_temp_pst10mts_data_flag',
    'avg_sea_sfc_temp_pst10mts_1',
    'avg_sea_sfc_temp_pst10mts_1_qa_summary',
    'avg_sea_sfc_temp_pst10mts_1_data_flag',
    'avg_wnd_spd_pst10mts',
    'avg_wnd_spd_pst10mts_qa_summary',
    'avg_wnd_spd_pst10mts_data_flag',
    'avg_wnd_spd_pst10mts_1',
    'avg_wnd_spd_pst10mts_1_qa_summary',
    'avg_wnd_spd_pst10mts_1_data_flag',
    'avg_wnd_spd_pst10mts_2',
    'avg_wnd_spd_pst10mts_2_qa_summary',
    'avg_wnd_spd_pst10mts_2_data_flag',
    'avg_wnd_dir_pst10mts',
    'avg_wnd_dir_pst10mts_qa_summary',
    'avg_wnd_dir_pst10mts_data_flag',
    'avg_wnd_dir_pst10mts_1',
    'avg_wnd_dir_pst10mts_1_qa_summary',
    'avg_wnd_dir_pst10mts_1_data_flag',
    'avg_wnd_dir_pst10mts_2',
    'avg_wnd_dir_pst10mts_2_qa_summary',
    'avg_wnd_dir_pst10mts_2_data_flag',
    'max_avg_wnd_spd_pst10mts',
    'max_avg_wnd_spd_pst10mts_qa_summary',
    'max_avg_wnd_spd_pst10mts_data_flag',
    'max_avg_wnd_spd_pst10mts_1',
    'max_avg_wnd_spd_pst10mts_1_qa_summary',
    'max_avg_wnd_spd_pst10mts_1_data_flag',
    'max_avg_wnd_spd_pst10mts_2',
    'max_avg_wnd_spd_pst10mts_2_qa_summary',
    'max_avg_wnd_spd_pst10mts_2_data_flag',
    'wnd_snsr_vert_disp',
    'wnd_snsr_vert_disp_qa_summary',
    'wnd_snsr_vert_disp_data_flag',
    'wnd_snsr_vert_disp_1',
    'wnd_snsr_vert_disp_1_qa_summary',
    'wnd_snsr_vert_disp_1_data_flag',
    'wnd_snsr_vert_disp_2',
    'wnd_snsr_vert_disp_2_qa_summary',
    'wnd_snsr_vert_disp_2_data_flag',
    'pk_wave_pd_pst20mts',
    'pk_wave_pd_pst20mts_qa_summary',
    'pk_wave_pd_pst20mts_data_flag',
    'pk_wave_pd_pst20mts_1',
    'pk_wave_pd_pst20mts_1_qa_summary',
    'pk_wave_pd_pst20mts_1_data_flag',
    'pk_wave_hgt_pst20mts',
    'pk_wave_hgt_pst20mts_qa_summary',
    'pk_wave_hgt_pst20mts_data_flag',
    'pk_wave_hgt_pst20mts_1',
    'pk_wave_hgt_pst20mts_1_qa_summary',
    'pk_wave_hgt_pst20mts_1_data_flag',
    'sig_wave_pd_pst20mts',
    'sig_wave_pd_pst20mts_qa_summary',
    'sig_wave_pd_pst20mts_data_flag',
    'sig_wave_pd_pst20mts_1',
    'sig_wave_pd_pst20mts_1_qa_summary',
    'sig_wave_pd_pst20mts_1_data_flag',
    'sig_wave_hgt_pst20mts',
    'sig_wave_hgt_pst20mts_qa_summary',
    'sig_wave_hgt_pst20mts_data_flag',
    'sig_wave_hgt_pst20mts_1',
    'sig_wave_hgt_pst20mts_1_qa_summary',
    'sig_wave_hgt_pst20mts_1_data_flag',
    'avg_wave_pd_pst20mts',
    'avg_wave_pd_pst20mts_qa_summary',
    'avg_wave_pd_pst20mts_data_flag',
    'avg_wave_pd_pst20mts_1',
    'avg_wave_pd_pst20mts_1_qa_summary',
    'avg_wave_pd_pst20mts_1_data_flag',
    'avg_wave_hgt_pst20mts',
    'avg_wave_hgt_pst20mts_qa_summary',
    'avg_wave_hgt_pst20mts_data_flag',
    'avg_wave_hgt_pst20mts_1',
    'avg_wave_hgt_pst20mts_1_qa_summary',
    'avg_wave_hgt_pst20mts_1_data_flag',
    'avg_max_wave_pd_pst20mts',
    'avg_max_wave_pd_pst20mts_qa_summary',
    'avg_max_wave_pd_pst20mts_data_flag',
    'avg_max_wave_pd_pst20mts_1',
    'avg_max_wave_pd_pst20mts_1_qa_summary',
    'avg_max_wave_pd_pst20mts_1_data_flag',
    'avg_max_wave_hgt_pst20mts',
    'avg_max_wave_hgt_pst20mts_qa_summary',
    'avg_max_wave_hgt_pst20mts_data_flag',
    'avg_max_wave_hgt_pst20mts_1',
    'avg_max_wave_hgt_pst20mts_1_qa_summary',
    'avg_max_wave_hgt_pst20mts_1_data_flag',
    'avg_mslp_pst10mts',
    'avg_mslp_pst10mts_qa_summary',
    'avg_mslp_pst10mts_data_flag',
    'avg_mslp_pst10mts_1',
    'avg_mslp_pst10mts_1_qa_summary',
    'avg_mslp_pst10mts_1_data_flag',
    'avg_wtr_lvl_snsr_volt_pst10mts',
    'avg_wtr_lvl_snsr_volt_pst10mts_qa_summary',
    'avg_wtr_lvl_snsr_volt_pst10mts_data_flag',
    'avg_wtr_lvl_snsr_volt_pst10mts_1',
    'avg_wtr_lvl_snsr_volt_pst10mts_1_qa_summary',
    'avg_wtr_lvl_snsr_volt_pst10mts_1_data_flag',
    'pres_tend_amt_pst3hrs',
    'pres_tend_amt_pst3hrs_qa_summary',
    'pres_tend_amt_pst3hrs_data_flag',
    'pres_tend_amt_pst3hrs_1',
    'pres_tend_amt_pst3hrs_1_qa_summary',
    'pres_tend_amt_pst3hrs_1_data_flag',
    'pres_tend_char_pst3hrs',
    'pres_tend_char_pst3hrs_qa_summary',
    'pres_tend_char_pst3hrs_data_flag',
]

# file & date/time formats
dt_format = '%Y%m%d'
file_name_format = '%s_%s.csv'  # station id and date

def insert_into_csv(line):
    'inserts dictionary into csv file'
    # sampling_time = date for file name output
    

    return 


def remove_and_log_uninsertable_keys(line):
    '''Remove and report any fields that are in the XML but arent in the database
       This shouldnt be needed, but just in case a new field is added at the source before
       we have a chance to add the column to the DB
    '''

    for k in list(line.keys()):
        if k not in column_headers:
            print(f"Cannot insert {k}")
            del line[k]


def read_buoys_list(filename):
    file = open(filename)
    buoys_list = file.read().strip().split('\n')
    file.close()
    return buoys_list


def ingest_buoy_xml_file(filename):
    'Insert buoy data in XML format into a database'

    this_dir = os.path.dirname(os.path.realpath(__file__))

    buoys_list = read_buoys_list(this_dir + '/' + buoy_filename)

    # get a flat dictionary from the source XML
    buoy_record = buoy_xml_to_json(filename)

    if buoy_record['wmo_synop_id'] not in buoys_list:
        print(
            f"Buoy {buoy_record['wmo_synop_id']} not in: {' '.join(buoys_list)}  ")
        return

    # remove and print extra fields that our table doesn't have (yet)
    # probably not necessary if we keep an eye on the mailing lists
    remove_and_log_uninsertable_keys(buoy_record)

    # insert the record
    return insert_into_csv(buoy_record)


@click.command()
@click.argument('filename', type=click.Path(exists=True))
def main(filename):

    res = ingest_buoy_xml_file(filename)

    if res:
        print(f'{res.rowcount} rows update or inserted in directory ')


if __name__ == '__main__':
    main()
