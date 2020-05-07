#Load data into HDFS
sh load_data_in_HDFS.sh

if [ $? -eq 0 ]
then
echo "Data imported to HDFS"
else
echo "Data import to HDFS failed"
exit 1
fi

#Execute pig script for converting checkins.json data into individual rows
pig -x mapreduce process_checkins_dataset_in_pig > output.msg 2> output.err

if [ $? -eq 0 ]
then
echo "Checkins data transformation completed"
else
echo "Checkins data transformation failed"
exit 1
fi


#Execute the pyspark script to transform the reviews and business data
spark-submit data_processing_and_transformations_in_spark.py

if [ $? -eq 0 ]
then
echo "business and reviews data transformation completed"
else
echo "business and reviews data transformation failed"
exit 1
fi

#Execute hive scripts to load the tranformed data from hdfs to a hive table
hive -f hive_script.hql

if [ $? -eq 0 ]
then
echo "business, reviews and checkins data loaded to hive"
else
echo "failed to load business, reviews and checkins data to hive"
exit 1
fi
