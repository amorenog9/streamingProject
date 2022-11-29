package es.upm.dit

object struct {
  case class UserEventPrompt(EVENT_TYPE_user: String, DATE_EVENT_user: String, ID_user: String, correctParams: Boolean)

  case class TrainEvent(EVENT_TYPE: String, DATE_EVENT: Long, ID: String)

}
