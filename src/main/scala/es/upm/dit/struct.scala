package es.upm.dit

object struct {
  case class UserEventPrompt(event_type_user: String, date_event_user: String, id_user: String, lat_user: String , lng_user: String , location_user: String ,correctParams: Boolean)

  case class TrainEvent(event_type: String, date_event: Long, id: String, lat: Double, lng: Double, location: String) //Es como me viene el evento train del topic

  case class TrainEventMemory(event_type: String, date_event: Long, id: String, coordinates: (Double, Double), location: String,
                              date_event_memory: List[Long], event_type_memory: List[String], position_memory: List[(Double, Double)]) //

}
