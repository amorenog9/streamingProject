package es.upm.dit

import com.typesafe.config.ConfigFactory

import sys.process._



/*
  * Herramienta auxiliar para resetear todos los kafkaTopics de manera rápida sin tener que
  * volver a desplegar todo docker

 */

object CleanKafkaTopics{
  def main(args: Array[String]) {

    // Config
    val parametros = ConfigFactory.load("applicationTrain.conf")
    val tipoProd = "local"

    var kafkaDir = ""
    if(tipoProd == "local") {
       kafkaDir = parametros.getString("KAFKA_DIR")
    }else{
       kafkaDir = parametros.getString("KAFKA_DIR_REMOTE")
    }


    val messages_in = "messages_in"
    val messages_out = "messages_out"
    val messages_out_no_memory = "messages_out_no_memory"
    val messages_from_timestamp_out = "messages_from_timestamp_out"


    "pkill -f TemporalStreamProducer.py".!
    "pkill -f TemporalStreamConsumer.py".!
    "pkill -f producer.py".!
    "pkill -f consumer.py".!


    s"${kafkaDir}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${messages_in}".!
    s"${kafkaDir}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${messages_out}".!
    s"${kafkaDir}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${messages_out_no_memory}".!
    s"${kafkaDir}/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${messages_from_timestamp_out}".!

    s"${kafkaDir}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${messages_in}".!
    s"${kafkaDir}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${messages_out}".!
    s"${kafkaDir}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${messages_out_no_memory}".!
    s"${kafkaDir}/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ${messages_from_timestamp_out}".!


  }
}
