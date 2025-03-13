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

    // Define the schema for the JSON message
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("modeName", StringType, nullable = true),
      StructField("lineStatus", StringType, nullable = true),
      StructField("statusSeverity", StringType, nullable = true),
      StructField("created", StringType, nullable = true),
      StructField("modified", StringType, nullable = true),
      StructField("serviceType", StringType, nullable = true)
    ))

    // Read the JSON messages from Kafka as a DataFrame
    val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", " ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092").option("subscribe", topic).option("startingOffsets", "latest").option("failOnDataLoss", "false").load().select(from_json(col("value").cast("string"), schema).as("data")).selectExpr("data.*")
    // Write the DataFrame as CSV files to HDFS
    df.writeStream.format("csv").option("checkpointLocation", "/tmp/jenkins/kafka/trainarrival/checkpoint").option("path", "/tmp/jenkins/kafka/trainarrival/data").start().awaitTermination()
  }

}
