export SPARK_HOME=/data/spark
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/spark_py_redis_kafka_stream_to_console.py | tee /home/workspace/spark/logs/redisstream.log
