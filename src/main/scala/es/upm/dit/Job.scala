package es.upm.dit

import java.util.Properties
import org.apache.flink.formats.json.JsonNodeDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import es.upm.dit.struct.{TrainEvent}


object Job{
  def main(args: Array[String]) {

    val KAFKA_TOPIC = "messages"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("group.id", "test")
    val kafkaConsumer = new FlinkKafkaConsumer(
      KAFKA_TOPIC,
      new JsonNodeDeserializationSchema(), //deserializes a JSON String into an ObjectNode.
      kafkaProperties)
    val train: DataStream[TrainEvent] = env
      .addSource(kafkaConsumer)//.setStartFromEarliest())
      .map(jsonNode => TrainEvent(
        EVENT_TYPE = jsonNode.get("EVENT_TYPE").asText(),
        DATE_EVENT = jsonNode.get("DATE_EVENT").asText(), // tengo que cambiarlo a fecha
        ID = jsonNode.get("ID").asText()
      ))

    val keyedListTrains = train.keyBy(_.ID)
      .flatMap(new EventProcessor())

    keyedListTrains.print()

    env.execute("FLink-Execution")


  }
}
