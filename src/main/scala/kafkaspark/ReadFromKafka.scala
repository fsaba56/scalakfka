package kafkaspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafkaToJson").master("local[*]").getOrCreate()

    // Define the Kafka parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-14-3.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // Define the Kafka topic to subscribe to
    val topic = "tfl_underground_Details"

    // Define the schema for the JSON messages
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("lineStatus", StringType, nullable = true),
      StructField("timedetails", TimestampType, nullable = false) // New column
                ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", " ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "latest").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")
    // Add the current timestamp column when reading the DataFrame
       val df1 = df.withColumn("timedetails", current_timestamp())
    // Write the DataFrame as CSV files to HDFS
        df1.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/tfl_underground/checkpoint").option("path", "/tmp/jenkins/kafka/tfl_underground/data").start().awaitTermination()
  }

}
