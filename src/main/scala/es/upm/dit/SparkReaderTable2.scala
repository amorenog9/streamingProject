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
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("bootstrap.servers", parametros.getString("KAFKA_DIRECTION_IN"))
      kafkaProperties.setProperty("group.id", "test")

      val kafkaConsumer = new FlinkKafkaConsumer(
        KAFKA_TOPIC_IN,
        new JsonNodeDeserializationSchema(), //deserializes a JSON String into an ObjectNode.
        kafkaProperties) //.setStartFromEarliest()

      val train: DataStream[TrainEvent] = env
        .addSource(kafkaConsumer) //.setStartFromLatest())
        .map(jsonNode => TrainEvent(
          id = jsonNode.get(s"${id}").asText(),
          event_type = jsonNode.get(s"${event_type}").asText(),
          date_event = jsonNode.get(s"${date_event}").asLong(), // fecha en epoch milliseconds
          lat = jsonNode.get(s"${lat}").asDouble(),
          lng = jsonNode.get(s"${lng}").asDouble(),
          location = jsonNode.get(s"${location}").asText()
        ))

      val KEY = train.keyBy(_.id)
        .flatMap(new EventProcessorFromTimestamp())


      // Sinks
      KEY.print()

      KEY
        .map(_.asJson.noSpaces)
        .addSink(new FlinkKafkaProducer[String](
          parametros.getString("KAFKA_DIRECTION_OUT"),
          KAFKA_TOPIC_OUT,
          new SimpleStringSchema))


      env.execute("FLink-Execution")





    println("HE ACABADOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
  }


}
