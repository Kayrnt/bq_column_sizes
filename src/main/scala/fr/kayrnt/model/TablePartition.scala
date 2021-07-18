package fr.kayrnt.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class TablePartition(
    partitionId: String,
    function: String
) {

  private val standardizedPartition = partitionId match {
    case "__UNPARTITIONED__" =>
      TimeFormatting.javaHourlyDateTimeFormatter.format(TimeFormatting.nowUTC)
    case _ =>
      TimeFormatting.formatPartitionToHourlyLevel(partitionId)
  }

  def toQueryCondition =
    s"$function('${TimeFormatting.bqHourlyFormat}', '$standardizedPartition')"

  def formattedForOutput: String =
    LocalDateTime
      .parse(standardizedPartition, TimeFormatting.javaHourlyDateTimeFormatter)
      .format(DateTimeFormatter.ISO_DATE_TIME)
}
