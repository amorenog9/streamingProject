package es.upm.dit

import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}

import java.io.File

import sys.process._


object KafkaSparkWriter{

  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val KAFKA_TOPIC_OUT = parametros.getString("KAFKA_TOPIC_OUT")
    val pathEvents = parametros.getString("DIR_EVENTS") // necesario para cambiar permisos chmod en /tmp/events
    val pathTrain = parametros.getString("TRAIN_DIR")
    val pathTable = parametros.getString("TRAIN_DIR_TABLE")
    val pathCheckpoint = parametros.getString("TRAIN_DIR_CHECKPOINT")

    /*
    // Borramos las trazas pasadas de la tabla de trenes
    println("¿Quieres borrar la ruta donde se almacena la tabla para crear una nueva?")
    print("s/n: ")
    val userConfirmation = scala.io.StdIn.readLine()

    if (userConfirmation == "s") {
      val dir = new File(pathTrain)
      if (dir.exists()) FileUtils.deleteDirectory(dir) // Cuidado con este
      println("Se ha eliminado la tabla existente")
    }
    else{println("No, se ha borrado la tabla. Se seguira excribiendo sobre la tabla existente")}
     */

    // Borramos las trazas pasadas las tablas y checkpoints
    val dir = new File(pathTrain)
    if (dir.exists()) FileUtils.deleteDirectory(dir) // Cuidado con este


    // Creamos una nueva ruta para almacenar la tabla
    val path = new File(pathTable).getAbsolutePath //directorio donde se va a almacenar la tabla

    // Creamos SparkSession
    val spark = SparkSession
      .builder
      .appName("Kafka2Delta")
      //.master("local[*]") // si es en distrib, quitar esta linea
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
        StructField("coordinates_memory", ArrayType(ArrayType(DoubleType))),
        StructField("location_memory", ArrayType(StringType))
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

    // Crea una tabla vacia con el esquema especificado
    val exists = DeltaTable.isDeltaTable(path)
    if (!exists) {
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      emptyDF
        .write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(path)
    }


    // Esta funcion nos permite actualizar el evento que tenemos en la tabla con el evento nuevo (si el id que le entra existe en la tabla => modifica la fila; Si no, metemos como nueva fila el evento que viene)
    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
      val deltaTable = DeltaTable.forPath(path)

      // Tomo el microbatchDF y agrupo los valores (puede haber varios con mismo ID si se procesa muy rapido los eventos) y tomo unicamente la fila con DATE_EVENT
      // mas grande (la ultima actualizacion del evento) de cada ID
      val maxDateEvent = microBatchOutputDF
        .groupBy($"id")
        .agg(functions.max($"date_event"))
        .toDF("r_id", "max_date_event")

      // Junto esas pocas filas con el dataframe grande microbatchDF para tener el resto de columnas de esas filas obtenidas en maxDateEvent === JOIN
      val joinedDF = microBatchOutputDF.join(
        maxDateEvent,
        ($"r_id" === $"id") && ($"max_date_event" === $"date_event")
      ).drop("r_id", "max_date_event") //elimino las columnas usadas en maxDateEvent para que no salgan tras hacer el JOIN

      /* Foto para ver como de verdad hay varias filas iguales en el mismo microBatch
      println("microbatch")
      microBatchOutputDF.show()

      println("joined")
      joinedDF.show()
      */

      deltaTable
        .as("t")
        .merge(
          joinedDF.alias("s"),
          "s.id = t.id"
        )
        .whenMatched().updateAll() //si hay coincidencia de mismo id (expresion merge) => actualiza la fila con lo que me viene de joinedDF
        .whenNotMatched().insertAll() //si no hay fila con ese id => inserta la fila
        .execute()
    }



    val query = ds
      .writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      //.outputMode("append")
      .option("checkpointLocation", new File(pathCheckpoint).getCanonicalPath)
      .start(path)

    s"chmod -R 777 ${pathEvents}".! //cambiamos los permisos de lectura y escritura de /tmp/events y sus subcarpetas
    println(s"Comenzamos a almacenar los eventos en la tabla Delta a traves de los mensajes que vienen del topic ${KAFKA_TOPIC_OUT}")


    query.awaitTermination()


  }
}

