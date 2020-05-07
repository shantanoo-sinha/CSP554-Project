checkin = LOAD '/yelp/checkin.json' USING JsonLoader('business_id:chararray, date:chararray');

-- split by ',' and create a row so we can dereference it later
-- first column is business_id, rest is flatten to make rows
split_data = foreach checkin generate business_id, flatten(TOKENIZE(date, ','));

-- Write final output to csv file
store split_data into '/yelp/checkins_by_date_by_business' using PigStorage(',');