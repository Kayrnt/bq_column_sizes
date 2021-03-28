package fr.kayrnt.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class Partition(
    partitionId: String,
    function: String,
    bqFormatString: String,
    javaFormatString: String
) {
  def toCondition = s"$function('$bqFormatString', '$partitionId')"
  def formattedOutput: String =
    LocalDateTime
      .parse(partitionId, DateTimeFormatter.ofPattern(javaFormatString))
      .format(DateTimeFormatter.ISO_DATE_TIME)
}

object Partition {

  private val defaultPartition = "2021010100"

  def apply(partitionId: String, function: String): Partition = {
    val toAppendForLocalDateTimeFormat = defaultPartition.substring(partitionId.length)
    Partition(partitionId + toAppendForLocalDateTimeFormat, function, "%Y%m%d%H", "yyyyMMddHH")
  }

}
