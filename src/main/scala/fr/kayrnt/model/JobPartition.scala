package fr.kayrnt.model

import com.google.cloud.bigquery.TimePartitioning
import fr.kayrnt.model.TablePartition.defaultPartition

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.chaining.scalaUtilChainingOps

trait JobLevel { def value: String }
object HourlyLevel  extends JobLevel { override val value: String = "hour"  }
object DailyLevel   extends JobLevel { override val value: String = "day"   }
object MonthlyLevel extends JobLevel { override val value: String = "month" }
object YearlyLevel  extends JobLevel { override val value: String = "year"  }

object JobLevel {
  def jobLevel(levelStr: String): JobLevel = levelStr match {
    case HourlyLevel.value  => HourlyLevel
    case DailyLevel.value   => DailyLevel
    case MonthlyLevel.value => MonthlyLevel
    case YearlyLevel.value  => YearlyLevel
  }

  def formatter: DateTimeFormatter = TimeFormatting.dateTimeFormatter(TimeFormatting.hourlyFormat)

}

case class JobPartition(partitionTime: LocalDateTime, level: JobLevel) {
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

  private val defaultPartition = "YYYY010100"

  def apply(partitionId: String, level: JobLevel): JobPartition = {
    val toAppendForLocalDateTimeFormat = defaultPartition.substring(partitionId.length)
    val formatter                      = JobLevel.formatter
    JobPartition(
      LocalDateTime.parse(partitionId + toAppendForLocalDateTimeFormat, formatter),
      level
    )
  }

  def apply(level: JobLevel): JobPartition =
    JobPartition(LocalDateTime.now(ZoneId.of("UTC")), level)

}
