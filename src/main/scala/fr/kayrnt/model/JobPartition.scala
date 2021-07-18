package fr.kayrnt.model

import com.google.cloud.bigquery.TimePartitioning

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class JobPartition(
    partitionTime: LocalDateTime,
    level: JobLevel
) {
  def formattedOutput: String = partitionTime.format(DateTimeFormatter.ISO_DATE_TIME)

  def timePartitioning: TimePartitioning.Type =
    level match {
      case HourlyLevel  => TimePartitioning.Type.HOUR
      case DailyLevel   => TimePartitioning.Type.DAY
      case MonthlyLevel => TimePartitioning.Type.MONTH
      case YearlyLevel  => TimePartitioning.Type.YEAR
    }
}

object JobPartition {

  def apply(partitionId: String, level: JobLevel): JobPartition = {
    val formatter = TimeFormatting.javaHourlyDateTimeFormatter
    JobPartition(
      LocalDateTime.parse(TimeFormatting.formatPartitionToHourlyLevel(partitionId), formatter),
      level
    )
  }

  def apply(level: JobLevel): JobPartition =
    JobPartition(TimeFormatting.nowUTC, level)

}
