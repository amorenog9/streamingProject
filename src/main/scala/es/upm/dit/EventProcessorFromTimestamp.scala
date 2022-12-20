package es.upm.dit

import es.upm.dit.struct.{TrainEvent, TrainEventMemory}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class EventProcessorFromTimestamp extends RichFlatMapFunction[TrainEvent, TrainEventMemory] {

  private var trainMemoryState: ValueState[TrainEventMemory] = _

  override def flatMap(input: TrainEvent, out: Collector[TrainEventMemory]): Unit = {

    // access the state value
    val tmpCurrentState = trainMemoryState.value

    // If it hasn't been used before, it will be null
    val currentState = if (tmpCurrentState != null) { // valor que tenemos antes de actualizar
      tmpCurrentState
    } else {
      TrainEventMemory(event_type = "-", date_event=  0, id= "-", coordinates = (0, 0), location= "-", date_event_memory= Nil, event_type_memory= Nil, coordinates_memory= Nil, location_memory = Nil)
    }

    // update the count
    val newState = input //nueva informacion que me llega


    val newMemoryTrain = TrainEventMemory(
      id = newState.id,
      event_type = newState.event_type,
      date_event = newState.date_event,
      coordinates = (newState.lat, newState.lng),
      location = newState.location,
      date_event_memory = currentState.date_event_memory :+ newState.date_event, //añado a la lista anterior el nuevo elemento (al final)
      event_type_memory = currentState.event_type_memory :+ newState.event_type,
      coordinates_memory = currentState.coordinates_memory :+ (newState.lat, newState.lng),
      location_memory = currentState.location_memory :+ newState.location
    )


    // Creacion de evento
    if ((currentState.event_type != newState.event_type) && (currentState.date_event <= newState.date_event) && (currentState == TrainEventMemory("-",  "-", 0, (0, 0), "-", Nil, Nil, Nil, Nil))) {

      trainMemoryState.update(newMemoryTrain) //actualizamos el estado (valor actual -> valor nuevo)
      out.collect(newMemoryTrain)
      println(s"Se ha CREADO el evento del tren ${input.id} con el evento ${newState.event_type}") // solo se imprime en consola

    }


    // Actualizacion de evento -- Solo actualizamos si Evento diferente y Fecha de evento diferente a la anterior => si tiene el mismo evento en la misma hora => coleccion de VINs - No actualizo el estado
    if ((currentState.event_type != newState.event_type) && (currentState.date_event <= newState.date_event) && (currentState != TrainEventMemory("-", "-", 0,  (0, 0), "-", Nil, Nil, Nil, Nil))) {

      trainMemoryState.update(newMemoryTrain) //actualizamos el estado (valor actual -> valor nuevo)
      out.collect(newMemoryTrain)
      println(s"Se ha ACTUALIZADO el evento del tren ${input.id} de ${currentState.event_type} a ${newState.event_type}") // solo se imprime en consola
      //trainState.clear() - Podría reutilizarse si se repite el ID
    }
  }

  override def open(parameters: Configuration): Unit = {
    trainMemoryState = getRuntimeContext.getState(
      new ValueStateDescriptor[TrainEventMemory]("event", createTypeInformation[TrainEventMemory])
    )
  }

}