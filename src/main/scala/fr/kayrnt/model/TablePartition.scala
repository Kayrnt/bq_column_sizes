package fr.kayrnt.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class TablePartition(
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

object TablePartition {

  private val defaultPartition = "YYYY010100"

  def apply(partitionId: String, function: String): TablePartition = {
    val toAppendForLocalDateTimeFormat = defaultPartition.substring(partitionId.length)
    TablePartition(partitionId + toAppendForLocalDateTimeFormat, function, "%Y%m%d%H", "yyyyMMddHH")
  }

}
