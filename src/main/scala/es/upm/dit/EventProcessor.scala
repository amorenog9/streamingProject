package es.upm.dit

import es.upm.dit.struct.TrainEvent
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class EventProcessor extends RichFlatMapFunction[TrainEvent, TrainEvent] {

  private var trainState: ValueState[TrainEvent] = _

  override def flatMap(input: TrainEvent, out: Collector[TrainEvent]): Unit = {

    // access the state value
    val tmpCurrentState = trainState.value

    // If it hasn't been used before, it will be null
    val currentState = if (tmpCurrentState != null) {
      tmpCurrentState
    } else {
      TrainEvent("-", 0, "-", 0, 0, "-")
    }

    // update the count
    val newState = input

    // Creacion de evento
    if ((currentState.event_type != newState.event_type) && (currentState.date_event != newState.date_event) && (currentState == TrainEvent("-", 0, "-", 0, 0, "-"))) {
      trainState.update(newState)
      out.collect(newState)
      println(s"Se ha CREADO el evento del tren ${input.id} con el evento ${newState.event_type}") // solo se imprime en consola
    }


    // Actualizacion de evento
    // si tiene el mismo evento en la misma hora => coleccion de VINs - No actualizo el estado
    if ((currentState.event_type != newState.event_type) && (currentState.date_event != newState.date_event) && (currentState != TrainEvent("-", 0, "-", 0, 0, "-"))) {
      trainState.update(newState)
      out.collect(newState)
      println(s"Se ha ACTUALIZADO el evento del tren ${input.id} de ${currentState.event_type} a ${newState.event_type}") // solo se imprime en consola
      //trainState.clear() - Podría reutilizarse si se repite el ID
    }
  }

  override def open(parameters: Configuration): Unit = {
    trainState = getRuntimeContext.getState(
      new ValueStateDescriptor[TrainEvent]("event", createTypeInformation[TrainEvent])
    )
  }

}