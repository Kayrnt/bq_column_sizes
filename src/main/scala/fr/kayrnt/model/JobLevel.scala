package fr.kayrnt.model

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

}
