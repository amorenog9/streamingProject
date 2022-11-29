package es.upm.dit
import es.upm.dit.struct.UserEventPrompt


class UserPrompt {

  def getPromptArgs(): UserEventPrompt = {
    // Cuando los eventos vengan de diferentes fuentes (trenes, aguas, etc) sera necesario identicar parametros clave
    println("Introduce el valor del campo que identifica univocamente a los eventos (ID)")
    val ID_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica el tipo de evento")
    val EVENT_TYPE_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica la fecha en la que ocurre el evento")
    val DATE_EVENT_user = scala.io.StdIn.readLine()

    val correctParams: Boolean =
      if ((ID_user != "") && (EVENT_TYPE_user != "") && (DATE_EVENT_user != "")) {
        true
      }
      else {false}

    UserEventPrompt(EVENT_TYPE_user, DATE_EVENT_user, ID_user, correctParams)
  }

}
