package es.upm.dit

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SparkReaderTable{


  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val pathTable = parametros.getString("TRAIN_DIR_TABLE")

    val spark = SparkSession
      .builder
      .appName("DeltaReader")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("delta")
      //.option("versionAsOf", 5)
      .load(pathTable)

    df.show()

  }
}
