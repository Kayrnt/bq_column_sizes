package fr.kayrnt.model

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object TimeFormatting {

  val bqHourlyFormat   = "%Y%m%d%H"
  val javaHourlyFormat = "yyyyMMddHH"

  private val defaultPartition = "YYYY010100"

  val javaHourlyDateTimeFormatter: DateTimeFormatter = dateTimeFormatter(javaHourlyFormat)

  private def dateTimeFormatter(pattern: String): DateTimeFormatter =
    DateTimeFormatter
      .ofPattern(pattern)
      .withZone(ZoneId.of("UTC"))

  def formatPartitionToHourlyLevel(partitionId: String): String = {
    val toAppendForLocalDateTimeFormat = defaultPartition.substring(partitionId.length)
    partitionId + toAppendForLocalDateTimeFormat
  }

  def nowUTC: LocalDateTime = LocalDateTime.now(ZoneId.of("UTC"))

}
