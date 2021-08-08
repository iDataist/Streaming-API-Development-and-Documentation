from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, split
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType
)


redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", StringType()),
                    ]
                )
            ),
        ),
    ]
)

customerJSONSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)


spark = SparkSession.builder.appName("customerRecord").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

redisServerRawStreamingDF = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr(
    "cast(value as string) value"
)

# create a temporary streaming view based on the streaming dataframe
redisServerStreamingDF.withColumn(
    "value", from_json("value", redisMessageSchema)
).select(col("value.*")).createOrReplaceTempView("RedisData")

zSetEntriesEncodedStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisData"
)

# base64 decoding encodedCustomer
zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer",
    unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"),
)

# parse JSON from customer that contains customer record data
zSetDecodedEntriesStreamingDF.withColumn(
    "customer", from_json("customer", customerJSONSchema)
).select(col("customer.*")).createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    "select * from CustomerRecords where email is not null and birthDay is    "
    " not null"
)

# We parse the birthdate to get just the year, that helps determine age
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-")
    .getItem(0)
    .alias("birthYear"),
)

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
