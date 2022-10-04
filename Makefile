data_summary.php :
	curl 'https://enforcedata.dol.gov/views/data_summary.php' -X POST --data-raw 'agency=osha' > $@

links.csv : data_summary.php
	cat $< | grep '.csv.zip' | perl -pe 's/^.*?<a href="(https:\/\/enfxfr.dol.gov\/..\/data_catalog\/.*.csv.zip)".*/\1/' > $@
