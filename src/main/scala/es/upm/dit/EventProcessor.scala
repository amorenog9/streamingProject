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
      TrainEvent("-", "-", "-")
    }

    // update the count
    val newState = input

    // Creacion de evento
    if ((currentState.EVENT_TYPE != newState.EVENT_TYPE) && (currentState.DATE_EVENT != newState.DATE_EVENT) && (currentState == TrainEvent("-", "-", "-"))) {
      trainState.update(newState)
      out.collect(newState)
      println(s"Se ha CREADO el evento del tren ${input.ID} con el evento ${newState.EVENT_TYPE}") // solo se imprime en consola
    }
    // Actualizacion de evento
    // si tiene el mismo evento en la misma hora => coleccion de VINs - No actualizo el estado
    if ((currentState.EVENT_TYPE != newState.EVENT_TYPE) && (currentState.DATE_EVENT != newState.DATE_EVENT) && (currentState != TrainEvent("-", "-", "-"))) {
      trainState.update(newState)
      out.collect(newState)
      println(s"Se ha ACTUALIZADO el evento del tren ${input.ID} de ${currentState.EVENT_TYPE} a ${newState.EVENT_TYPE}") // solo se imprime en consola
      //trainState.clear() - Podría reutilizarse si se repite el ID
    }
  }

  override def open(parameters: Configuration): Unit = {
    trainState = getRuntimeContext.getState(
      new ValueStateDescriptor[TrainEvent]("event", createTypeInformation[TrainEvent])
    )
  }

}