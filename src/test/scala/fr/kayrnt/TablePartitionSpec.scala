package fr.kayrnt

import com.google.cloud.bigquery.{Field, StandardSQLTypeName, TimePartitioning}
import fr.kayrnt.model.TablePartition
import fr.kayrnt.reader.Partitions
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.Duration

class TablePartitionSpec extends AnyWordSpec with Matchers {

  "Partition" should {
    "compute formattedOutput properly on hourly partitions" in {
      TablePartition("2021010100", "").formattedForOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on daily partitions" in {
      TablePartition("20210101", "").formattedForOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on monthly partitions" in {
      TablePartition("202101", "").formattedForOutput should be("2021-01-01T00:00:00")
    }
    "compute formattedOutput properly on yearly partitions" in {
      TablePartition("2021", "").formattedForOutput should be("2021-01-01T00:00:00")
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
        "testField"
      ) should be(Some("PARSE_TIMESTAMP"))
    }
    "compute getPartitioningFieldTypeFunction properly on dates" in {
      Partitions.getPartitioningFieldTypeFunction(
        Seq(
          Field.of("testField", StandardSQLTypeName.TIMESTAMP),
          Field.of("testField2", StandardSQLTypeName.DATE)
        ),
        "testField2"
      ) should be(Some("PARSE_DATE"))
    }

  }
}
