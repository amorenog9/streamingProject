package es.upm.dit

object struct {

  case class TrainEvent(
  EVENT_TYPE: String,
  DATE_EVENT: Long, // tengo que cambiarlo a fecha
  ID: String)

}
