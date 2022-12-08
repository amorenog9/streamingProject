package es.upm.dit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object SparkReaderTable{


  def main(args: Array[String]) {

    // Quitamos los mensajes de Info
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val spark = SparkSession
      .builder
      .appName("DeltaReader")
      .master("local[*]")
      .getOrCreate()

    // in production this should be a more reliable location such as HDFS
    val path = "/tmp/delta/table"

    val df = spark.read
      .format("delta")
      //.option("versionAsOf", 5)
      .load(path)

    df.show()

  }
}
