# MSC Datamart Tools

buoy_xml_parser-> Database script

This repo contains scripts to receive and parse realtime marine moored-buoy data from MSC Datamart and put it into a database. It receives data using the AMQP system described [here](https://eccc-msc.github.io/open-data/msc-datamart/amqp_en/) . Also includes ERDDAP config to get it from the database into ERDDAP.

## Installation

1. From this directory, install the buoy loading script with

`pip install -e .`

2. Test that the package is installed by running `python -m msc_ingest.parse_xml sample.xml` from this directory

3. Edit dd_swob_marine.conf as needed (optional)

4. Create a postgres database and run `db.sql` to create a table

5. Change this line in `msc_ingest/ingest_to_db.py`: `db_string = "postgres://user:pass@host:5432/database"` to reflect your DB settings.

6. Test that it works by running `python msc_ingest/ingest_to_db.py sample.xml` from this directory. This should create a new record in your table. Note that running this multiple times will not produce multiple records.

7. Start recording data to the database with
   `sr_subscribe start dd_swob_marine.conf`

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

## Links

MSC Datamart Documentation
https://dd.weather.gc.ca/observations/doc/

Sarracenia
https://github.com/MetPX/sarracenia

## TODO

How to only accept certain buoys?
