# MSC Datamart Tools

buoy_xml_parser-> Database script

This repo contains scripts to receive and parse realtime marine moored-buoy data from MSC Datamart and put it into a database. It receives data using the AMQP system described [here](https://eccc-msc.github.io/open-data/msc-datamart/amqp_en/) . Also includes ERDDAP config to get it from the database into ERDDAP.

## Installation

1. From this directory, install the buoy loading script with

`pip install -e .`

2. Edit dd_swob_marine.conf

3. Create database tables and connect to it

4. start recording data to the database with
   `sr_subscribe start dd_swob_marine.conf`

## Just parsing an XML file

After installing,

```sh
cd buoy_data_ingest
python parse_xml.py ../sample.xml
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
