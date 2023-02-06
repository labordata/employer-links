establishment/learned_settings : osha_enforcement.csv whd.csv | whd_entities.csv
	python scripts/osha_link.py $^

gazetteer.db : canonical.csv entity_map.csv
	csvs-to-sqlite $^ $@
	sqlite-utils transform $@ canonical --pk id
	sqlite-utils create-index $@ entity_map id entity_id --unique
	sqlite-utils add-foreign-key $@ entity_map id canonical id

canonical.csv : whd_entities.csv
	csvcut -C entity_id,confidence_score $< > $@

entity_map.csv : whd_entities.csv
	csvcut -c entity_id,confidence_score,id $< > $@

whd_entities.csv : whd.csv
	python scripts/whd_dedupe.py $< $@

whd.csv :
	wget -O $@ "https://labordata.bunkum.us/whisard-de1d2cf.csv?sql=with+lowered+as+(%0D%0A++select%0D%0A++++case_id%2C%0D%0A++++lower(trade_nm)+as+trade_nm%2C%0D%0A++++lower(legal_name)+as+legal_name%2C%0D%0A++++lower(street_addr_1_txt)+as+street_addr_1_txt%2C%0D%0A++++lower(cty_nm)+as+cty_nm%2C%0D%0A++++lower(st_cd)+as+st_cd%2C%0D%0A++++zip_cd%2C%0D%0A++++naic_cd%2C%0D%0A++++findings_start_date%2C%0D%0A++++findings_end_date%0D%0A++from%0D%0A++++whisard%0D%0A)%0D%0Aselect%0D%0A++*%0D%0Afrom%0D%0A++lowered%0D%0Agroup+by%0D%0A++trade_nm%2C%0D%0A++legal_name%2C%0D%0A++street_addr_1_txt%2C%0D%0A++cty_nm%2C%0D%0A++st_cd%2C%0D%0A++zip_cd%2C%0D%0A++naic_cd%3B&_size=max&_dl=1"

osha_enforcement.csv :
	wget -O $@ "https://labordata.bunkum.us/osha_enforcement-f313f47.csv?sql=with+lowered+as+(%0D%0A++select%0D%0A++++activity_nr%2C%0D%0A++++lower(estab_name)+as+trade_nm%2C%0D%0A++++lower(estab_name)+as+legal_name%2C%0D%0A++++lower(site_address)+as+street_addr_1_txt%2C%0D%0A++++lower(site_city)+as+cty_nm%2C%0D%0A++++lower(site_state)+as+st_cd%2C%0D%0A++++naics_code+as+naic_cd%0D%0A++from%0D%0A++++inspection%0D%0A)%2C%0D%0Auniques+as+(%0D%0A++select%0D%0A++++*%0D%0A++from%0D%0A++++lowered%0D%0A++group+by%0D%0A++++trade_nm%2C%0D%0A++++legal_name%2C%0D%0A++++street_addr_1_txt%2C%0D%0A++++cty_nm%2C%0D%0A++++st_cd%2C%0D%0A++++naic_cd%0D%0A)%0D%0Aselect%0D%0A++*%0D%0Afrom%0D%0A++uniques%0D%0Aorder+by%0D%0A++random()%0D%0Alimit%0D%0A++10000%3B&_size=max&_dl=1"
