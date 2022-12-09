package es.upm.dit

import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File


object KafkaSparkWriter{

  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val parametros = ConfigFactory.load("applicationTrain.conf")
    val KAFKA_TOPIC_OUT = parametros.getString("KAFKA_TOPIC_OUT")

    // Borramos las trazas pasadas de la tabla
    if (new File("/tmp/delta").exists()) FileUtils.deleteDirectory(new File("/tmp/delta")) // cuidado con esta linea
    val path = new File("/tmp/delta/table").getAbsolutePath

    val spark = SparkSession
      .builder
      .appName("Kafka2Delta")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // Para upsertToDelta necesitamos una delta table (con las columnas del evento) => Vamos a crear una fila en esa tabla simplemente para que el resto de codigo pueda referenciarse a ella.
    // Para ello, spark va a leer de un topic diferente a los utilizados en el programa
    val dfTableCreation = spark.read.json("/home/alex/Escritorio/TFM/flink_pruebasb/src/main/scala/es/upm/dit/JsonTable.json") // tengo que mover este Json RUTA RELATIVA!
    dfTableCreation.write.format("delta").save(path)

    // Leemos eventos del topic de Kafka
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", KAFKA_TOPIC_OUT)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
    // df.printSchema()

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

    println("Esquema que tendra nuestra Delta Table:")
    ds.printSchema()


    // Esta funcion nos permite actualizar el evento que tenemos en la tabla con el evento nuevo (si el id que le entra existe en la tabla => modifica la fila; Si no, metemos como nueva fila el evento que viene)
    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
      val deltaTable = DeltaTable.forPath(path)
      deltaTable
        .as("t")
        .merge(
          microBatchOutputDF.alias("s"),
          "s.id = t.id"
        )
        .whenMatched("t.event_type != s.event_type").updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    println(s"Comenzamos a almacenar los eventos en la tabla Delta a traves de los mensajes que vienen del topic ${KAFKA_TOPIC_OUT}")

    val query = ds
      .writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      //.outputMode("append")
      .option("checkpointLocation", new File("/tmp/delta/checkpoint").getCanonicalPath)
      .start(path)

    query.awaitTermination()


  }
}

