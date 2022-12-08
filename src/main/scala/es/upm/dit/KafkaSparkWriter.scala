package es.upm.dit

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.format.ListType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import java.io.File


object KafkaSparkWriter{


  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val parametros = ConfigFactory.load("applicationTrain.conf")
    val KAFKA_TOPIC_OUT = parametros.getString("KAFKA_TOPIC_OUT")


    // in production this should be a more reliable location such as HDFS
    if (new File("/tmp/delta").exists()) FileUtils.deleteDirectory(new File("/tmp/delta")) // cuidado con esta linea
    val path = new File("/tmp/delta/table").getAbsolutePath


    val spark = SparkSession
      .builder
      .appName("Kafka2Delta")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // Leemos eventos del topic de Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KAFKA_TOPIC_OUT)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
    df.printSchema()

    // Creamos el esquema de nuestro JSON para llevarlo a SPARK
    val schema = StructType(
      List(
        StructField("id", StringType),
        StructField("event_type", StringType),
        StructField("date_event", LongType),
        StructField("coordinates", ArrayType(DoubleType)),
        StructField("location", StringType),
        StructField("date_event_memory", ArrayType(LongType)),
        StructField("event_type_memory", ArrayType(StringType)),
        StructField("position_memory", ArrayType(ArrayType(DoubleType)))
      ))

    /*
        // Tomamos las columnas que queremos almacenar de lo que nos viene de Kafka a la BBDD
        val ds = df.select(col("key"), $"value" cast "string" as "json")
          .select(col("key"), from_json($"json", schema) as "data")
          .select(col("key"), col("data.*"))

        ds.printSchema()
    */

    // Tomamos las columnas que queremos almacenar de lo que nos viene de Kafka a la BBDD
    val ds = df.select($"value" cast "string" as "json")
      .select( from_json($"json", schema) as "data")
      .select(col("data.*"))

    ds.printSchema()

    val query = ds
      .writeStream
      // .outputMode("append")
      .format("delta")
      .option("checkpointLocation", new File("/tmp/delta/checkpoint").getCanonicalPath)
      .start(path)

    query.awaitTermination()


  }
}

