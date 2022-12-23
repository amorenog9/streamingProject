package es.upm.dit

import es.upm.dit.struct.TrainEvent
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class EventProcessorFromTimestamp extends RichFlatMapFunction[TrainEvent, TrainEvent] {

  private var trainMemoryState3: ValueState[TrainEvent] = _

  override def flatMap(input: TrainEvent, out: Collector[TrainEvent]): Unit = {

    // access the state value
    val tmpCurrentState = trainMemoryState3.value

    // If it hasn't been used before, it will be null
    val currentState = if (tmpCurrentState != null) { // valor que tenemos antes de actualizar
      tmpCurrentState
    } else {
      TrainEvent(event_type = "-", date_event=  0, id= "-", lat =0, lng= 0, location= "-")
    }

    // update the count
    val newState = input //nueva informacion que me llega

    val newMemoryTrain = TrainEvent(
      id = newState.id,
      event_type = newState.event_type,
      date_event = newState.date_event,
      lat = newState.lat,
      lng =  newState.lng,
      location = newState.location
    )


    // Creacion de evento
    if ((currentState.event_type != newState.event_type) && (currentState.date_event <= newState.date_event) && (currentState == TrainEvent("-",  "-", 0, 0, 0, "-"))) {

      trainMemoryState3.update(newMemoryTrain) //actualizamos el estado (valor actual -> valor nuevo)
      out.collect(newMemoryTrain)
      println(s"Se ha CREADO el evento del tren ${input.id} con el evento ${newState.event_type}") // solo se imprime en consola

    }


    // Actualizacion de evento -- Solo actualizamos si Evento diferente y Fecha de evento diferente a la anterior => si tiene el mismo evento en la misma hora => coleccion de VINs - No actualizo el estado
    if ((currentState.event_type != newState.event_type) && (currentState.date_event <= newState.date_event) && (currentState != TrainEvent("-", "-", 0, 0, 0, "-"))) {

      trainMemoryState3.update(newMemoryTrain) //actualizamos el estado (valor actual -> valor nuevo)
      out.collect(newMemoryTrain)
      println(s"Se ha ACTUALIZADO el evento del tren ${input.id} de ${currentState.event_type} a ${newState.event_type}") // solo se imprime en consola
      //trainState.clear() - Podría reutilizarse si se repite el ID
    }
  }

  override def open(parameters: Configuration): Unit = {
    trainMemoryState3 = getRuntimeContext.getState(
      new ValueStateDescriptor[TrainEvent]("event2", createTypeInformation[TrainEvent])
    )
  }

}