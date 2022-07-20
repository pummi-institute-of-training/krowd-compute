Steps to create the model.json from reviews-topic-composition-spark.txt

1. login to this server
ssh -i "krowd.pem" ubuntu@ec2-54-174-158-11.compute-1.amazonaws.com

2. put reviews-topic-composition-spark.txt and VoljinCityModelGenerator_combined.scala files at
location /mnt

3. move to 
cd spark-2.0.1-bin-hadoop2.7 folder

4. run this command to start the scala shell
./bin/spark-shell --conf spark.executor.extraClassPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.executor.extraLibraryPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.driver.extraClassPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.driver.extraLibraryPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --packages com.github.nscala-time:nscala-time_2.10:2.6.0,com.databricks:spark-csv_2.10:1.3.0,cc.mallet:mallet:2.0.7 --repositories https://dl.bintray.com/derrickburns/maven/ --conf spark.kryoserializer.buffer.max=512m


5. run the command
scala> :load /mnt/Krowd-Restaurant/scripts/model_generation/VoljinCityModelGenerator.scala

