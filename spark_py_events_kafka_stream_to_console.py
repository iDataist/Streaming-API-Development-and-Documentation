from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    DateType,
)

eventSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType()),
    ]
)

spark = SparkSession.builder.appName("customerEvent").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stediEventRawStreamingDF = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# cast each field from a binary to a string
stediEventStreamingDF = stediEventRawStreamingDF.selectExpr(
    "cast(value as string) value"
)

# create a temporary streaming view based on the streaming dataframe
stediEventStreamingDF.withColumn(
    "value", from_json("value", eventSchema)
).select(col("value.*")).createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerRiskStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
