import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    ArrayType,
    DoubleType,
    LongType,
)

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


from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

spark_udf = udf(transform_array, ArrayType(DoubleType()))

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaBatchJob").getOrCreate()

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-service:9092")
    input_topic = os.getenv("INPUT_TOPIC", "input-events")
    output_topic = os.getenv("OUTPUT_TOPIC", "output-events")

    # -----------------------------
    # Read Kafka topic (batch)
    # -----------------------------
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # -----------------------------
    # Parse JSON & apply transformation
    # -----------------------------
    parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    transformed = parsed.withColumn(
        "Properties.data_array", spark_udf(col("Properties.data_array"))
    )

    # -----------------------------
    # Write back to Kafka
    # -----------------------------
    out = transformed.selectExpr(
        "CAST(Restaurantid AS STRING) AS key", "to_json(struct(*)) AS value"
    )

    out.write.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap).option(
        "topic", output_topic
    ).save()

    print("Batch processing completed.")
