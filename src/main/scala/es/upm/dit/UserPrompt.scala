package es.upm.dit
import es.upm.dit.struct.UserEventPrompt


class UserPrompt {

  def getPromptArgs(): UserEventPrompt = {
    // Cuando los eventos vengan de diferentes fuentes (trenes, aguas, etc) sera necesario identicar parametros clave
    println("Introduce el valor del campo que identifica univocamente a los eventos (ID)")
    val id_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica el tipo de evento")
    val event_type_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica la fecha en la que ocurre el evento")
    val date_event_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica latitud")
    val lat_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica la longitud")
    val lng_user = scala.io.StdIn.readLine()

    println("Introduce el valor del campo que identifica la localizacion")
    val location_user = scala.io.StdIn.readLine()

    val correctParams: Boolean =
      if ((id_user != "") && (event_type_user != "") && (date_event_user != "") && (lat_user != "") && (lng_user != "") && (location_user != "") ) {
        true
      }
      else {false}

    UserEventPrompt(event_type_user, date_event_user, id_user, lat_user, lng_user, location_user, correctParams)
  }

}
