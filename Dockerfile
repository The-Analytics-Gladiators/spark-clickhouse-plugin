FROM bitnami/spark
COPY target/scala-2.12/spark-clickhouse-plugin-assembly-0.0.1.jar /opt/bitnamy/spark/jars/
CMD spark-submit --class com.blackmorse.spark.clickhouse.Main /opt/bitnamy/spark/jars/spark-clickhouse-plugin-assembly-0.0.1.jar