package es.upm.dit

import scala.util.matching.Regex

object TimeUtils extends App{

  //val desiredTime = "01/02/2017 18:00:00" //Input de usuario
  // Se podría dividir en mas campos

  var dateCheck: Boolean = false
  var hourCheck: Boolean = false

  println("Introduce dia lectura: dd/mm/yyyy")
  val dayMonthYear = scala.io.StdIn.readLine()
  println("Introduce hora lectura: HH/mm/ss")
  val hourMinSec = scala.io.StdIn.readLine()


  var desiredTime: String = ""


  // el valor 0+[1-9], el valor 1 o 2 + [0-9], el valor 3+[0 o 1]
  val datePattern: Regex = "^(0[1-9]|[12][0-9]|3[01])[- /.](0[1-9]|1[012])[- /.](19|20)\\d\\d$".r
  datePattern.findFirstMatchIn(dayMonthYear) match {
    case Some(_) => dateCheck = true
    case None => println("Fecha invalida")
  }

  val hourMinSecPattern: Regex = "^(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d)$".r
  hourMinSecPattern.findFirstMatchIn(hourMinSec) match {
    case Some(_) => hourCheck = true
    case None => println("Hora invalida")
  }


  if (dateCheck && hourCheck) {
    desiredTime = dayMonthYear + " " + hourMinSec
  } else {
    println("Es necesario escribir la fecha en el formato indicado")
  }


  val format = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
  val time = format.parse(desiredTime).getTime()
  println(time)

  // https://medium.com/@ZeevFeldbeine/how-to-start-spark-structured-streaming-by-a-specific-kafka-timestamp-e4b0a3e9c009
  // https://spark.apache.org/docs/3.0.0-preview/structured-streaming-kafka-integration.html


}
