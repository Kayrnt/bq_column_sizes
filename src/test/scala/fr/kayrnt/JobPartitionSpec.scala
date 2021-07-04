package fr.kayrnt

import fr.kayrnt.model._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{LocalDateTime, ZoneId}

class JobPartitionSpec extends AnyWordSpec with Matchers {

  "JobPartition" should {
    "compute formattedOutput properly on hourly partitions for provided partition" in {
      JobPartition("2021010100", HourlyLevel).formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on daily partitions for provided partition" in {
      JobPartition("20210101", DailyLevel).formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on monthly partitions for provided partition" in {
      JobPartition("202101", MonthlyLevel).formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on yearly partitions for provided partition" in {
      JobPartition("2021", YearlyLevel).formattedOutput should be("2021-01-01T00:00:00")
    }

    val ldt = LocalDateTime.now(ZoneId.of("UTC"))

    "compute formattedOutput properly on hourly partitions with level only" in {
      val jp = JobPartition(HourlyLevel).partitionTime
      jp.getYear should equal(ldt.getYear)
      jp.getMonthValue should equal(ldt.getMonthValue)
      jp.getDayOfYear should equal(ldt.getDayOfYear)
      jp.getHour should equal(ldt.getHour)
    }
    "compute formattedOutput properly on daily partitions with level only" in {
      val jp = JobPartition(DailyLevel).partitionTime
      jp.getYear should equal(ldt.getYear)
      jp.getMonthValue should equal(ldt.getMonthValue)
      jp.getDayOfYear should equal(ldt.getDayOfYear)
    }
    "compute formattedOutput properly on monthly partitions with level only" in {
      val jp = JobPartition(MonthlyLevel).partitionTime
      jp.getYear should equal(ldt.getYear)
      jp.getMonthValue should equal(ldt.getMonthValue)
    }
    "compute formattedOutput properly on yearly partitions with level only" in {
      val jp = JobPartition(YearlyLevel).partitionTime
      jp.getYear should equal(ldt.getYear)
    }

  }
}
