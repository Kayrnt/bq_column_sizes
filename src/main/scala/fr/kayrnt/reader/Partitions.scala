package fr.kayrnt.reader

import cats.effect.IO
import cats.effect.std.Semaphore
import com.google.cloud.bigquery.{BigQuery, Field, Job, JobInfo, QueryJobConfiguration, StandardSQLTypeName, TimePartitioning}
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.model.Partition

import scala.jdk.CollectionConverters._
import scala.util.Try

object Partitions extends LazyLogging {

  def extractPartitioningType(partition: String): TimePartitioning.Type = partition.length match {
    case 4  => TimePartitioning.Type.YEAR
    case 6  => TimePartitioning.Type.MONTH
    case 8  => TimePartitioning.Type.DAY
    case 10 => TimePartitioning.Type.HOUR
  }

  def formatPartition(partition: String): String =
    if (partition.forall(c => Try(Integer.valueOf(c)).toOption.isDefined)) {
      s"$partition%"
    } else
      throw new IllegalArgumentException(
        s"Partition format isn't matching expected input (received $partition), please check documentation."
      )

  def getPartitions(
      bq: BigQuery,
      sem: Semaphore[IO],
      tableReference: String,
      partitioningFieldTypeFunction: String,
      partitionOpt: Option[String]
  ): IO[List[Partition]] = {
    val whereClause = partitionOpt
      .map(partition =>
        s"""\nWHERE partition_id LIKE "${formatPartition(partition)}" """
      ).getOrElse("")
    val query = s"""#legacySQL
                   |SELECT
                   |  partition_id
                   |FROM
                   |  [$tableReference$$__PARTITIONS_SUMMARY__] $whereClause
                   |order by partition_id desc
                   |""".stripMargin
    logger.debug("query : " + query)
    runQuery(bq, sem, query).map { j =>
      j.getQueryResults().iterateAll().asScala.map { v =>
          val value = v.get(0).getStringValue
          Partition(value, partitioningFieldTypeFunction)
        }.toList
    }
  }

  def runQuery(bq: BigQuery, sem: Semaphore[IO], queryString: String): IO[Job] = {
    val config = QueryJobConfiguration
      .newBuilder(queryString)
      .setUseLegacySql(true)
      .build()
    sem.permit.use { _ =>
      IO(bq.create(JobInfo.of(config)))
    }
  }

  def getPartitioningFieldTypeFunction(
      fields: Seq[Field],
      partitioningField: String,
      partitioningType: TimePartitioning.Type
  ): String = {

    val partitioningFieldType =
      fields
        .find(f => f.getName == partitioningField)
        .map(f => f.getType.getStandardType)
        .getOrElse {
          partitioningField match {
            case "_PARTITIONTIME" => StandardSQLTypeName.TIMESTAMP
            case "_PARTITIONDATE" => StandardSQLTypeName.DATE
            case _ =>
              throw new IllegalStateException(
                s"Unable to find type for partitioning field ($partitioningField) / partitioning type ($partitioningType) "
              )
          }
        }

    partitioningFieldType match {
      case StandardSQLTypeName.TIMESTAMP => "PARSE_TIMESTAMP"
      case StandardSQLTypeName.DATE      => "PARSE_DATE"
      case _ =>
        throw new IllegalStateException(
          s"Unsupported type $partitioningFieldType"
        )
    }

  }

}
