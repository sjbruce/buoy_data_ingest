# MSC Datamart Tools

This repo contains scripts to receive and parse realtime marine moored-buoy data from MSC Datamart and put it into a database. It receives data using the AMQP system described [here](https://eccc-msc.github.io/open-data/msc-datamart/amqp_en/) . Also includes ERDDAP config to get it from the database into ERDDAP.

## Installation

1. `cd` into this directory

1. If needed, install virtualenv.

   `pip install virtualenv --user`

1. Create and activate new virtual environment

   ```sh
      virtualenv -p python3 venv
      source venv/bin/activate
   ```

1. Install the buoy loading script with

   `pip install -e .`

1. Test that the package is installed by running `python -m msc_ingest.parse_xml sample.xml` from this directory, this should return a JSON representation of the data in the sample.xml

### Ingesting into a PostgreSQL database

1. Edit `dd_swob_marine.conf` as needed, eg to change temp directory
1. Create a postgres database and run `db.sql` to create a table, and GRANT permissions to a user to read/write
1. Edit `msc_ingest/buoy_list.txt` with a list of buoys you want. See 'Buoy list' below.
1. Change this line in `msc_ingest/ingest_to_db.py`: `db_string = "postgres://user:pass@host:5432/database"` to reflect your DB settings.
1. Test that it works by running `python msc_ingest/ingest_to_db.py sample.xml` from this directory. This should create a new record in your table. Note that running this multiple times will not produce multiple records.
1. Start recording data to the database with
   `sr_subscribe start dd_swob_marine.conf`

### Ingesting into a series of CSV files

1. Edit `dd_swob_marine_csv.conf` as needed, change destination directory, accept filter, etc.

2. Edit `msc_ingest/buoy_list.txt` with a list of buoys you want. See 'Buoy list' below.

3. Edit `msc_ingest/ingest_to_csv.conf`:

   1. `buoy_list` = path to `buoy_list.txt`
   2. `output_dir` = path to the desired output directory/folder
   3. `column_headers` = a comma separated list of column names that will keep the headers & columns consistent should anything change in the SWOB-ML XML
   4. `index_field` = the field name that will be used to set the index, assumed to be a date/time field in ISO format
   5. `check_duplicates_field` = field used to determine duplicate records when processing records
   6. `station_id_field` = which field is the station identifier
   7. `datetime_format` = date/time format string, used to determine the final output file name (% must be escaped, i.e. represented as %%)
   8. `filename_format` = the filename format string, it is assumed that two values will be inserted into the string the information in the station_id_field and the date/time of the index field represented by the datetime_format field.

4. Test by running `python msc_ingest/ingest_to_csv.py sample.xml` from this directory. This should create a new record in your destination directory.

   **Notes:** 

   1. running this multiple times will not produce multiple records;
   2. the `--bulk` option processes all files in a supplied directory instead of one specific file;
   3. the `--config_file` option will allow you to specify the path to a configuration file instead of assuming `msc_ingest/ingest_to_csv.conf`;
   4. the `--log` option will allow you to specify the path to a log file, `ingest_to_csv.log` is assumed as the default when run independently from Sarracenia;

5. Start recording data with `sr_subscribe start dd_swob_marine_csv.conf`

## Just parsing an XML file

After installing,

```sh
python -m msc_ingest.parse_xml sample.xml
```

## ERDDAP

The datasets.xml provided should cover all of the possible fields in the data, based on the

## Troubleshooting

Sarracenia logging:
`sr_subscribe log dd_swob_marine.conf`

## Buoy list

At time of writing there are 3 Atlantic and 2 Pacific buoys available in this system:

Pacific:
46303
46304

Atlantic:
44488
44489
44490

## Links

MSC Datamart Documentation
https://dd.weather.gc.ca/observations/doc/

Sarracenia
https://github.com/MetPX/sarracenia

Raw XML files that get pushed into this system. Also find list of currently published buoys here
https://dd.weather.gc.ca/observations/swob-ml/marine/moored-buoys
