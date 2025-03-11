import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.kafka.common.TopicPartition

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",  // or "latest" if you want to get new messages
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "uttam_tfl"
    val partitionId = 0 // Partition to consume
    
    // Define the Kafka topic and partition to consume from
    val topicPartition = new TopicPartition(topic, partitionId)

    // Read data from Kafka, specify the topic and partition explicitly
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092")
      .option("assign", s"""[{"topic":"$topic", "partition":$partitionId}]""") // Use "assign" for specific partition
      .option("startingOffsets", "earliest")  // Change to "latest" if needed
      .load()
      .selectExpr("CAST(value AS STRING)") // Read the value column as a String

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

    // Parse JSON and flatten the structure
    val parsedDF = df.select(from_json(col("value"), schema).as("data")).selectExpr("data.*")

    // Write to console for debugging
    parsedDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
