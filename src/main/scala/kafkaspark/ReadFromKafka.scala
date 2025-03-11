package kafkaspark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Kafka topic and partition to subscribe to
    val topic = "uttam_tfl"
    val partitionId = 0 // Partition to consume

    // Define schema for the incoming JSON
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

    // Read data from Kafka specifying the partitionId
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
      .load()

    // Extract the "value" column, which contains the raw message, and parse it into structured data
    val df = kafkaStream.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    // Filter by partitionId to ensure only data from the specified partition is processed
    val partitionFilteredDF = df.filter(col("partition") === partitionId)

    // Write data to console (for testing purposes)
    partitionFilteredDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
