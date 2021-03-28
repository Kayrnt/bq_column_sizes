package fr.kayrnt

import fr.kayrnt.model.Partition
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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
  }
}
