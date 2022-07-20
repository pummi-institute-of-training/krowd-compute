cd /mnt/spark-2.0.1-bin-hadoop2.7

./bin/spark-shell --conf spark.executor.extraClassPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.executor.extraLibraryPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.driver.extraClassPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --conf spark.driver.extraLibraryPath=/root/spark/lib/mysql-connector-java-5.1.38-bin.jar --packages com.github.nscala-time:nscala-time_2.10:2.6.0,com.databricks:spark-csv_2.10:1.3.0,cc.mallet:mallet:2.0.7 --repositories https://dl.bintray.com/derrickburns/maven/ --conf spark.kryoserializer.buffer.max=512m

:load /mnt/Krowd-Restaurant/scripts/model_generation/VoljinCityModelGenerator.scala
