package fr.kayrnt.model

import java.time.ZoneId
import java.time.format.DateTimeFormatter

object TimeFormatting {

  def dateTimeFormatter(partition: String): DateTimeFormatter =
    DateTimeFormatter
      .ofPattern(partition)
      .withZone(ZoneId.of("UTC"))

  val hourlyFormat: String = "yyyyMMddHH"

}
