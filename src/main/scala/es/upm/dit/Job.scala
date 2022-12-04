package es.upm.dit

import java.util.Properties
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala._
import es.upm.dit.struct._
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

import org.apache.flink.api.common.serialization.SimpleStringSchema




object Job{
  def main(args: Array[String]) {

    val KAFKA_TOPIC_IN = "messages_in"
    val KAFKA_TOPIC_OUT = "messages_out"


    //val userArguments = new UserPrompt().getPromptArgs() // para introducir los parametros por el terminal
    val userArguments = UserEventPrompt("EVENT_TYPE", "DATE_EVENT", "ID", "LAT", "LNG", "LOCATION_IDENTIFIED", true)

    println(s"Los parametros introducidos son: ${userArguments.id_user}, ${userArguments.event_type_user}, ${userArguments.date_event_user}, ${userArguments.lat_user}, ${userArguments.lng_user}, ${userArguments.location_user}")
    println("Comienza flink-streaming")

    if (userArguments.correctParams){
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
      kafkaProperties.setProperty("group.id", "test")

      val kafkaConsumer = new FlinkKafkaConsumer(
        KAFKA_TOPIC_IN,
        new JsonNodeDeserializationSchema(), //deserializes a JSON String into an ObjectNode.
        kafkaProperties)

      val train: DataStream[TrainEvent] = env
        .addSource(kafkaConsumer)//.setStartFromLatest())
        .map(jsonNode => TrainEvent(
          event_type = jsonNode.get(s"${userArguments.event_type_user}").asText(),
          date_event = jsonNode.get(s"${userArguments.date_event_user}").asLong(), // fecha en epoch milliseconds
          id = jsonNode.get(s"${userArguments.id_user}").asText(),
          lat = jsonNode.get(s"${userArguments.lat_user}").asDouble(),
          lng = jsonNode.get(s"${userArguments.lng_user}").asDouble(),
          location = jsonNode.get(s"${userArguments.location_user}").asText()
        ))

      val keyedListTrains = train.keyBy(_.id)
        .flatMap(new EventProcessor())


      // Sinks
      keyedListTrains.print()

      keyedListTrains
        .map(_.asJson.noSpaces)
        .addSink(new FlinkKafkaProducer[String](
        "localhost:9092",
          KAFKA_TOPIC_OUT,
        new SimpleStringSchema))


      env.execute("FLink-Execution")
    }

    else{println(s"Los parametros introducidos no son validos: ${userArguments}")}


  }
}
