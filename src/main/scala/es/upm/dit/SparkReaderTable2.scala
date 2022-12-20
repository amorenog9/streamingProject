package es.upm.dit
import com.typesafe.config.ConfigFactory

import java.util.Properties
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema}
import org.apache.flink.streaming.api.scala._
import es.upm.dit.struct._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path

import scala.sys.process._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.parquet.format.LogicalType.JSON





object SparkReaderTable2{

  def main(args: Array[String]) {


    val parametros = ConfigFactory.load("applicationTrain.conf")

    val pythonScriptFile = parametros.getString("PATH_EVENTS_FROM_TIMESTAMP_KAFKA_PRODUCER")
    val savePathEventsFromTimestamp = parametros.getString("PATH_EVENTS_FROM_TIMESTAMP")
    val timeStampValue = 1L

    val KAFKA_TOPIC_IN = parametros.getString("TIMESTAMP_TOPIC_IN")
    val KAFKA_TOPIC_OUT = parametros.getString("TIMESTAMP_TOPIC_OUT")

    val id = parametros.getString("idEvento")
    val event_type = parametros.getString("nombreEvento")
    val date_event = parametros.getString("fechaEvento")
    val lat = parametros.getString("latitudEvento")
    val lng = parametros.getString("longitudEvento")
    val location = parametros.getString("localizacionEvento")


  // -------------------------------------------------------------------------------------------
  //script para limpiar el kafka topic temporal (tanto de entrada como se salida) !!!!
  // -------------------------------------------------------------------------------------------


    //Ejecutamos el script que realiza el stream de los eventos posteriores al timestamp almacenados en la BBDD
    s"python3 ${pythonScriptFile}".! //con ! bloqueamos hasta que termine de enviarse lo del script; con run se paraleliza https://www.scala-lang.org/files/archive/api/current/scala/sys/process/ProcessBuilder.html


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = savePathEventsFromTimestamp + s"/${timeStampValue}" + "/eventsFromTimestamp.json"
    val inputFormat = new TextInputFormat(new Path(path));
    inputFormat.setCharsetName("UTF-8");
    val ds = env.readFile(inputFormat, path)


    val keyedListTrains = ds
      .flatMap(new EventProcessorFromTimestamp())



    ds.print()





    env.execute("jobname02");




    println(json)

  }


}
