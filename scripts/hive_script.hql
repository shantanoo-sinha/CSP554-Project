
--Create Database yelp
create database yelp;

--Set current database as yelp
use yelp;

--Drop table checkins_by_date_by_businesses
--drop table yelp.checkins_by_date_by_businesses;

--Create table for storing checkins by date for each business
CREATE TABLE yelp.checkins_by_date_by_businesses
(
	business_id varchar(100),
	checkin_date timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--Load the data from HDFS into the created hive table
load data inpath '/yelp/checkins_by_date_by_business/part*' into table yelp.checkins_by_date_by_businesses;

--Check data
--select * from yelp.checkins_by_date_by_businesses limit 10;



--Drop table business_reviews
drop table yelp.business_reviews;

--Create table for storing business reviews
CREATE TABLE yelp.business_reviews
(
	business_id varchar(100),
	review_id varchar(100),
	review_date timestamp,
	useful int,
	stars float,
	city varchar(100),
	state varchar(100),
	latitude float,
	longitude float,
	business_name varchar(50),
	postal_code varchar(5)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--Load the data from HDFS into the created hive table
load data inpath '/yelp/business_reviews/*.csv' into table yelp.business_reviews;

--Check data
--select * from yelp.business_reviews limit 10;


--Create table for storing tips
CREATE TABLE yelp.tips
(
	user_id varchar(100),
	business_id varchar(100),
	text varchar(100),
	tip_date timestamp,
    compliment_count int	
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--Load the data from HDFS into the created hive table
load data inpath '/yelp/yelp_tips/part*' into table yelp.tips;

--Check data
--select * from yelp.tips limit 10;


--Create table for storing user
CREATE TABLE yelp.yelp_user
(
	user_id varchar(100),
	name varchar(100),
	review_count int,
	yelping_since timestamp,
	friends varchar(1000),
	useful int,
	funny int,
	cool int,
	fans int,
	elite varchar(1000),
	average_stars float,
	compliment_hot int,
	compliment_more int,
	compliment_profile int,
	compliment_cute int,
	compliment_list int,
	compliment_note int,
	compliment_plain int,
	compliment_cool int,
	compliment_funny int,
	compliment_writer int,
	compliment_photos int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--Load the data from HDFS into the created hive table
load data inpath '/yelp/yelp_user/part*' into table yelp.yelp_user;

--Check data
--select * from yelp.yelp_user limit 10;