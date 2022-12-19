package es.upm.dit

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{arrays_zip, col, explode, expr, flatten, size}

import java.io.File




object SparkReaderTable{


  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val pathTable = parametros.getString("TRAIN_DIR_TABLE")

    val savePathEventsFromTimestamp = parametros.getString("EVENTS_FROM_TIMESTAMP")

    val event_type = parametros.getString("nombreEvento")
    val date_event = parametros.getString("fechaEvento")
    val lat = parametros.getString("latitudEvento")
    val lng= parametros.getString("longitudEvento")
    val location= parametros.getString("localizacionEvento")


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
        ("first", Array(1, 2, 3), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6)), Array("Sevilla", "Andorra", "Cadiz")),
        ("test", Array(2, 4, 6), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6)), Array("Sevilla", "Andorra", "Cadiz")),
        ("choose", Array(4, 6, 8), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6)), Array("Sevilla", "Andorra", "Cadiz")),
        ("d", Array(-1, 0, 1), Array("A", "B", "C"), Array(Array(1,2), Array(3,4), Array(5,6)), Array("Sevilla", "Andorra", "Cadiz"))
      )
    )
    val df = spark.createDataFrame(rdd).toDF("id", "date_event_memory", "event_type_memory", "coordinates_memory", "location_memory")

    df.show()


    // Parametro relacionado con la query
    val timeStampValue = 2



    // Este DF proporciona la ultima actualización de evento hasta la fecha seleccionada (incluyendo la fecha seleccionada)
    val actualDf = df.select(col("*"), expr(s"filter(date_event_memory, x -> x <= ${timeStampValue})").as("dates_before_timestamp"))
      .withColumn("index_size_before_date", size(col("dates_before_timestamp"))) //el array index empieza en 1 en spark array. Esta columna nos da el indice que empezar a leer event_type_memory
      .filter(size(col("dates_before_timestamp")) > 0)//eliminamos aquellas filas que no tengan eventos hasta la fecha seleccionada
      .withColumn("date_before_timestamp", functions.expr("slice (date_event_memory , index_size_before_date, 1)")) //tomamos unicamente 1 elemento empezando por el valor index_size_before_date (es decir, el ultimo hasta la fecha timeStampValue)
      .withColumn("event_before_timestamp", functions.expr("slice (event_type_memory , index_size_before_date, 1)"))
      .withColumn("coordinate_before_timestamp", functions.expr("slice (coordinates_memory , index_size_before_date, 1)"))
      .withColumn("location_before_timestamp", functions.expr("slice (location_memory , index_size_before_date, 1)"))
      .drop("date_event_memory")
      .drop("event_type_memory")
      .drop("coordinates_memory")
      .drop("location_memory")
      .drop("dates_before_timestamp")
      .drop("index_size_before_date")
      .withColumn(s"${date_event}", functions.expr("explode(date_before_timestamp)")) // transformo [ELEMENTO] a Elemento (quito el array)
      .withColumn(s"${event_type}", functions.expr("explode(event_before_timestamp)")) // transformo [ELEMENTO] a Elemento (quito el array)
      .withColumn(s"${location}", functions.expr("explode(location_before_timestamp)")) // transformo [ELEMENTO] a Elemento (quito el array)
      .withColumn("coordinate", functions.expr("flatten(coordinate_before_timestamp)")) // quitar con flatten 1 dimension del array (para que sea igual que como se reciben los eventos)
      .drop("date_before_timestamp")
      .drop("event_before_timestamp")
      .drop("coordinate_before_timestamp")
      .drop("location_before_timestamp")
      .withColumn(s"${lat}", col("coordinate").getItem(0)) // [lat, lng] lo transformo a lat
      .withColumn(s"${lng}", col("coordinate").getItem(1)) // [lat, lng] lo transformo a lng
      .drop("coordinate")


    /*
      .select(col("*"), flatten(col("event_before_timestamp")))
      .select(col("*"), flatten(col("coordinate_before_timestamp")))
      .select(col("*"), flatten(col("location_before_timestamp")))
       */

    actualDf.show()


    // Este df filtra desde la fecha seleccionada (sin incluirse), los eventos que tiene en memoria la tabla hasta el momento en el que se ha stremeado a la BBDD
    val df2 = df.select(col("*"), expr(s"filter(date_event_memory, x -> x > ${timeStampValue})").as("dates_from_timestamp"))
      .withColumn("index_size_from_date", size(col("date_event_memory"))-size(col("dates_from_timestamp"))+1) //el array index empieza en 1 en spark array. Esta columna nos da el indice que empezar a leer event_type_memory
      .withColumn("events_from_timestamp", functions.expr("slice (event_type_memory , index_size_from_date, 999999999)")) //999999999 the length of the slice (valor maximo que podemos poner) -- suponemos infinito
      .withColumn("coordinates_from_timestamp", functions.expr("slice (coordinates_memory , index_size_from_date, 999999999)")) //coordenadas
      .withColumn("locations_from_timestamp", functions.expr("slice (location_memory , index_size_from_date, 999999999)")) //localizaciones
      .filter(size(col("dates_from_timestamp")) > 0)//eliminamos aquellas filas que no tengan eventos desde la fecha seleccionada
      .drop("index_size_from_date")
      .drop("date_event_memory") //borramos la columna que almacenaba las fechas (ahora solo queremos los eventos a partir de x fecha)
      .drop("event_type_memory") //borramos la columna que almacenaba los eventos (ahora solo queremos los eventos a partir de x fecha)
      .drop("coordinates_memory") //borramos la columna que almacenaba las posiciones (ahora solo queremos los eventos a partir de x fecha)
      .drop("location_memory")

    df2.show()


    // Este desglosa los arrays (memory) en diferentes columnas y despues ordena por fecha del evento para poder streamearlo de arriba hacia abajo (como el fichero .feather)
    val df3 = df2.withColumn("zip_values", functions.expr("arrays_zip( dates_from_timestamp, events_from_timestamp, coordinates_from_timestamp, locations_from_timestamp) "))
      .withColumn("zip_values", functions.expr("explode(zip_values)"))
      .drop("dates_from_timestamp")
      .drop("events_from_timestamp")
      .drop("coordinates_from_timestamp")
      .drop("locations_from_timestamp")
      .select(col("*"), col("zip_values.dates_from_timestamp"), col("zip_values.events_from_timestamp"), col("zip_values.coordinates_from_timestamp"), col("zip_values.locations_from_timestamp"))
      .drop("zip_values")
      .withColumn(s"${lat}", col("coordinates_from_timestamp").getItem(0))
      .withColumn(s"${lng}", col("coordinates_from_timestamp").getItem(1))
      .drop("coordinates_from_timestamp")
      .withColumnRenamed("dates_from_timestamp", s"${date_event}")
      .withColumnRenamed("events_from_timestamp", s"${event_type}")
      .withColumnRenamed("locations_from_timestamp", s"${location}")
      .orderBy(s"${date_event}") //antes de enviar la tabla a JSON -> la ordenamos para que se haga el stream correctamente


    // opcion de almacenar los eventos posteriores ordenados para enviarlos a un archivo
    df3.show()

    // Si existe el directorio - lo borramos
    val dir = new File(savePathEventsFromTimestamp + s"/${timeStampValue}")
    if (dir.exists()) FileUtils.deleteDirectory(dir) // Cuidado con este

    val result = df3.select(col("*")).toJSON.cache()
    val write = result.coalesce(1).toJavaRDD.saveAsTextFile(savePathEventsFromTimestamp + s"/${timeStampValue}")

    println(s"Se ha almacenado el fichero con eventos posteriores a ${timeStampValue} en la carpeta: ${savePathEventsFromTimestamp + s"/${timeStampValue}"}")



    val dfx = spark.read
      .format("delta")
      //.option("versionAsOf", 5)
      .load(pathTable)

    dfx.show()

  }


}
