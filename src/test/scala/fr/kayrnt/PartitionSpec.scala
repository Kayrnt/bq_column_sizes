package fr.kayrnt

import com.google.cloud.bigquery.{Field, StandardSQLTypeName, TimePartitioning}
import fr.kayrnt.model.Partition
import fr.kayrnt.reader.Partitions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class PartitionSpec extends AnyWordSpec with Matchers {

  "Partition" should {
    "compute formattedOutput properly on hourly partitions" in {
      Partition("2021010100", "").formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on daily partitions" in {
      Partition("20210101", "").formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on monthly partitions" in {
      Partition("202101", "").formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on yearly partitions" in {
      Partition("2021", "").formattedOutput should be("2021-01-01T00:00:00")
    }
    "compute applyOffset properly on hourly" in {
      Partitions.applyOffset("2021010100", Duration("1hour")) should be("2020123123")
    }
    "compute applyOffset properly on daily" in {
      Partitions.applyOffset("20210101", Duration("1hour")) should be("20201231")
    }
    "compute applyOffset properly on monthly" in {
      Partitions.applyOffset("202101", Duration("1hour")) should be("202012")
    }
    "compute applyOffset properly on yearly" in {
      Partitions.applyOffset("2021", Duration("1hour")) should be("2020")
    }
    "compute getPartitioningFieldTypeFunction properly on timestamps" in {
      Partitions.getPartitioningFieldTypeFunction(
        Seq(
          Field.of("testField", StandardSQLTypeName.TIMESTAMP),
          Field.of("testField2", StandardSQLTypeName.DATE)
        ),
        "testField",
        TimePartitioning.Type.DAY
      ) should be(Some("PARSE_TIMESTAMP"))
    }
    "compute getPartitioningFieldTypeFunction properly on dates" in {
      Partitions.getPartitioningFieldTypeFunction(
        Seq(
          Field.of("testField", StandardSQLTypeName.TIMESTAMP),
          Field.of("testField2", StandardSQLTypeName.DATE)
        ),
        "testField2",
        TimePartitioning.Type.DAY
      ) should be(Some("PARSE_DATE"))
    }

  }
}
