package es.upm.dit

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, expr, size}




object SparkReaderTable{


  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val pathTable = parametros.getString("TRAIN_DIR_TABLE_2")

    val spark = SparkSession
      .builder
      .appName("DeltaReader")
      .master("local[*]")
      //.master("spark://alex-GE75-Raider-8SF:7077")
      .getOrCreate()
    /*
        val df = spark.read
          .format("delta")
          //.option("versionAsOf", 5)
          .load(pathTable)

        df.show()


     */
    val sc = spark.sparkContext // Just used to create test RDDs

    val rdd = sc.parallelize(
      Seq(
        ("first", Array(1, 2, 3), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6))),
        ("test", Array(2, 4, 6), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6))),
        ("choose", Array(4, 6, 8), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6))),
        ("d", Array(-1, 0, 1), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6)))
      )
    )

    val df = spark.createDataFrame(rdd).toDF("id", "date_event_memory", "event_type_memory", "position_memory")

    df.show()


    val timeStampValue = 2



    // Este DF proporciona la ultima actualización de evento hasta la fecha seleccionada (incluyendo la fecha seleccionada)

    val actualDf = df.select(col("*"),
      expr(s"filter(date_event_memory, x -> x <= ${timeStampValue})").as("dates_before_timestamp"))
      .withColumn("index_size_before_date", size(col("dates_before_timestamp"))) //el array index empieza en 1 en spark array. Esta columna nos da el indice que empezar a leer event_type_memory
      .filter(size(col("dates_before_timestamp")) > 0)//eliminamos aquellas filas que no tengan eventos hasta la fecha seleccionada
      .withColumn("date_before_timestamp", functions.expr("slice (date_event_memory , index_size_before_date, 1)"))
      .withColumn("event_before_timestamp", functions.expr("slice (event_type_memory , index_size_before_date, 1)"))
      .withColumn("position_before_timestamp", functions.expr("slice (position_memory , index_size_before_date, 1)"))
      .drop("date_event_memory")
      .drop("event_type_memory")
      .drop("position_memory")
      .drop("dates_before_timestamp")
      .drop("index_size_before_date")

    actualDf.show()


    // Este df filtra desde la fecha seleccionada (sin incluirse), los eventos que tiene en memoria la tabla hasta el momento en el que se ha stremeado a la BBDD
    val df2 = df.select(col("*"),
      expr(s"filter(date_event_memory, x -> x > ${timeStampValue})").as("dates_from_timestamp"))
      .withColumn("index_size_from_date", size(col("date_event_memory"))-size(col("dates_from_timestamp"))+1) //el array index empieza en 1 en spark array. Esta columna nos da el indice que empezar a leer event_type_memory
      .withColumn("events_from_timestamp", functions.expr("slice (event_type_memory , index_size_from_date, 999999999)")) //999999999 the length of the slice (valor maximo que podemos poner) -- suponemos infinito
      .withColumn("positions_from_timestamp", functions.expr("slice (position_memory , index_size_from_date, 999999999)")) //coordenadas
      .filter(size(col("dates_from_timestamp")) > 0)//eliminamos aquellas filas que no tengan eventos desde la fecha seleccionada
      .drop("index_size_from_date")
      .drop("date_event_memory") //borramos la columna que almacenaba las fechas (ahora solo queremos los eventos a partir de x fecha)
      .drop("event_type_memory") //borramos la columna que almacenaba los eventos (ahora solo queremos los eventos a partir de x fecha)
      .drop("position_memory") //borramos la columna que almacenaba las posiciones (ahora solo queremos los eventos a partir de x fecha)

    df2.show()


  }


}
