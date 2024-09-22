Created production ready etl pipline using tools:-
===================================================
1).Apache Airflow for scheduling.
2).Sqoop for transferring between mysql database and hdfs.
3).Pyspark for filtering the data.
4).Create hive tables on the data.
5).Imported the final pipeline in hbase.

Steps for creating the pipeline:-
=================================
->Imported data from various sources like S3 , Mysql database into hdfs.
->Used http sensor in airflow for monitoring the data in s3 bucket.
->Filter the data using spark.
->Load the final result og pipeline into the hbase.
