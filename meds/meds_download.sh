#!/bin/sh

# Download and extracts CSV zips from MEDS. Only downloads when a newer zip file is available.

set -e

# set folder to save ZIPs
zip_folder=./zip

# set folder to save CSVs
csv_folder=./csv

# set file with lis of buoys
buoy_file=./buoys_download.txt

while read -r p; do
   wget --restrict-file-names=lowercase -N --ignore-case --directory-prefix $zip_folder "http://www.meds-sdmm.dfo-mpo.gc.ca/alphapro/wave/waveshare/csvData/${p}_csv.zip"
   # sleep 1
done  < $buoy_file

# extract to lower case, only extract if file has changed
unzip -LL -u "$zip_folder/*_csv.zip" -d $csv_folder

echo "Fixing up CSV headers.."

# Remove trailing comma from first line of each file
gsed -i "1,1s/,\W*$//" $csv_folder/*.csv

# Columns ATMS,GSPD,WSPD,WDIR are duplicated so I added '2' 
# set this as the new header
gsed -i "1c\STN_ID,DATE,Q_FLAG,LATITUDE,LONGITUDE,DEPTH,VCAR,VTPK,VWH\$,VCMX,VTP\$,WDIR,WSPD,WSS\$,GSPD,WDIR2,WSPD2,WSS\$2,GSPD2,ATMS,ATMS2,DRYT,SSTP" $csv_folder/c*.csv

echo "Done"