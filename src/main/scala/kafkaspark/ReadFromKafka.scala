package kafkaspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "uttam_tfl"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("stationName", StringType, nullable = true),
      StructField("lineName", StringType, nullable = true),
      StructField("towards", StringType, nullable = true),
      StructField("expectedArrival", StringType, nullable = true),
      StructField("vehicleId", StringType, nullable = true),
      StructField("platformName", StringType, nullable = true),
      StructField("direction", StringType, nullable = true),
      StructField("destinationName", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("timeToStation", StringType, nullable = true),
      StructField("currentLocation", StringType, nullable = true),
      StructField("timeToLive", StringType, nullable = true)
    ))

    // Read Kafka stream, which will give us a "value" column of type bytes
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Extract the "value" field from Kafka and parse the JSON data
    val df = kafkaStream.selectExpr("CAST(value AS STRING)") // Convert byte array to string
      .select(from_json(col("value"), schema).as("data")) // Parse JSON using the schema
      .select("data.*") // Flatten the nested data structure

    // Write the stream data to a different output format (parquet for testing)
    df.writeStream
      .format("parquet")
      .option("checkpointLocation", "/tmp/jenkins/kafka/trainarrival/checkpoint")
      .option("path", "/tmp/jenkins/kafka/trainarrival/data")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
