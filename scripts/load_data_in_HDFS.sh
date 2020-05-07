# create yelp directory to store the yelp dataset files
hdfs dfs -mkdir /yelp

# Import yelp dataset - review.json file in HDFS
hadoop fs -put review.json /yelp/

# Import yelp dataset - business.json file in HDFS
hadoop fs -put business.json /yelp/

# Import yelp dataset - checkin.json file in HDFS
hadoop fs -put checkin.json /yelp/

# Import yelp dataset - tip.json file in HDFS
hadoop fs -put tip.json /yelp/

# Import yelp dataset - user.json file in HDFS
hadoop fs -put user.json /yelp/