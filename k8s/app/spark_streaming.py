import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_json,
    struct,
    udf,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    ArrayType,
    DoubleType,
    LongType,
    TimestampType,
)
import os

# -----------------------------
# Schema
# -----------------------------
schema = StructType(
    [
        StructField("Restaurantid", LongType()),
        StructField("Event", StringType()),
        StructField(
            "Properties",
            StructType(
                [
                    StructField("timestamp", StringType()),
                    StructField("is_relevant", BooleanType()),
                    StructField("data_array", ArrayType(DoubleType())),
                ]
            ),
        ),
    ]
)


# -----------------------------
# Transformation function
# -----------------------------
def transform_array(arr):
    return [x * 2 for x in arr] if arr else []


spark_udf = udf(transform_array, ArrayType(DoubleType()))

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaSparkStructuredStreaming").getOrCreate()

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")
    input_topic = os.getenv("INPUT_TOPIC", "input-events")
    output_topic = os.getenv("OUTPUT_TOPIC", "output-events")

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # transformed = (
    #     parsed.withColumn("ts", to_timestamp(col("Properties.timestamp")))
    #     .filter(col("ts") >= current_timestamp())
    #     .withColumn("Properties.data_array", spark_udf(col("Properties.data_array")))
    # )

    transformed = parsed.withColumn(
        "ts", to_timestamp(col("Properties.timestamp"))
    ).withColumn("Properties.data_array", spark_udf(col("Properties.data_array")))

    out = transformed.selectExpr(
        "CAST(Restaurantid AS STRING) AS key", "to_json(struct(*)) AS value"
    )

    query = (
        out.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", output_topic)
        .option("checkpointLocation", "/tmp/checkpoints")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
