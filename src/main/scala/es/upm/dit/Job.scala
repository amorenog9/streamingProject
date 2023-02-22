package es.upm.dit

import com.typesafe.config.ConfigFactory

import java.util.Properties
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala._
import es.upm.dit.struct._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.flink.api.common.serialization.SimpleStringSchema




object Job{
  def main(args: Array[String]) {

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val KAFKA_TOPIC_IN = parametros.getString("KAFKA_TOPIC_IN")
    val KAFKA_TOPIC_OUT = parametros.getString("KAFKA_TOPIC_OUT")
    val KAFKA_TOPIC_OUT_NO_MEMORY = parametros.getString("KAFKA_TOPIC_OUT_NO_MEMORY")


    val userArguments = new UserPrompt().getPromptArgs(parametros) // para introducir los parametros por el terminal
    println(s"Los parametros introducidos son: ${userArguments.id_user}, ${userArguments.event_type_user}, ${userArguments.date_event_user}, ${userArguments.lat_user}, ${userArguments.lng_user}, ${userArguments.location_user}")
    println(s"Comienza flink-streaming en ${parametros.getString("nombreSistema")}")

    if (userArguments.correctParams){
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("bootstrap.servers", parametros.getString("KAFKA_DIRECTION_IN"))
      kafkaProperties.setProperty("group.id", "test")

      val kafkaConsumer = new FlinkKafkaConsumer(
        KAFKA_TOPIC_IN,
        new JsonNodeDeserializationSchema(), //deserializes a JSON String into an ObjectNode.
        kafkaProperties).setStartFromEarliest()

      val train: DataStream[TrainEvent] = env
        .addSource(kafkaConsumer)
        .map(jsonNode => TrainEvent(
          id = jsonNode.get(s"${userArguments.id_user}").asText(),
          event_type = jsonNode.get(s"${userArguments.event_type_user}").asText(),
          date_event = jsonNode.get(s"${userArguments.date_event_user}").asLong(), // fecha en epoch milliseconds
          lat = jsonNode.get(s"${userArguments.lat_user}").asDouble(),
          lng = jsonNode.get(s"${userArguments.lng_user}").asDouble(),
          location = jsonNode.get(s"${userArguments.location_user}").asText()
        ))

      val keyedMemoryTrains = train.keyBy(_.id)
        .flatMap(new EventProcessorToMemory())


      val keyedTrains = train.keyBy(_.id)
        .flatMap(new EventProcessor())


      // Sinks
      keyedMemoryTrains.print()
      //keyedTrains.print()

      keyedMemoryTrains
        .map(_.asJson.noSpaces)
        .addSink(new FlinkKafkaProducer[String](
          parametros.getString("KAFKA_DIRECTION_OUT"),
          KAFKA_TOPIC_OUT,
          new SimpleStringSchema))

      keyedTrains
        .map(_.asJson.noSpaces)
        .addSink(new FlinkKafkaProducer[String](
          parametros.getString("KAFKA_DIRECTION_OUT"),
          KAFKA_TOPIC_OUT_NO_MEMORY,
          new SimpleStringSchema))


      env.execute("Flink-Execution")
    }

    else{println(s"Los parametros introducidos no son validos: ${userArguments}")}


  }
}
