package fr.kayrnt.reader

import cats.effect.IO
import cats.effect.std.Semaphore
import com.google.cloud.bigquery.{BigQuery, Field, Job, JobInfo, QueryJobConfiguration, StandardSQLTypeName, TimePartitioning}
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.model.{TablePartition, TimeFormatting}

import java.time.format.DateTimeFormatter
import scala.jdk.DurationConverters._
import java.time.{LocalDate, LocalDateTime, ZoneId, ZoneOffset}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Try

object Partitions extends LazyLogging {

  def extractPartitioningTypeFromPartition(partition: String): TimePartitioning.Type =
    partition.length match {
      case 4  => TimePartitioning.Type.YEAR
      case 6  => TimePartitioning.Type.MONTH
      case 8  => TimePartitioning.Type.DAY
      case 10 => TimePartitioning.Type.HOUR
    }

  def applyOffset(partition: String, offset: Duration): String = {

    val finiteOffset: FiniteDuration = offset match {
      case f: FiniteDuration => f
      case _                 => throw new IllegalArgumentException(s"Unexpected offset $offset should be finite")
    }

    val instant = partition.length match {
      case 4 => LocalDate.of(partition.toInt, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC)
      case 6 =>
        LocalDate
          .of(
            partition.substring(0, 4).toInt,
            partition.substring(4, 6).toInt,
            1
          ).atStartOfDay().toInstant(ZoneOffset.UTC)
      case 8 =>
        LocalDate
          .of(
            partition.substring(0, 4).toInt,
            partition.substring(4, 6).toInt,
            partition.substring(6, 8).toInt
          ).atStartOfDay().toInstant(ZoneOffset.UTC)
      case 10 =>
        LocalDateTime
          .of(
            partition.substring(0, 4).toInt,
            partition.substring(4, 6).toInt,
            partition.substring(6, 8).toInt,
            partition.substring(8, 10).toInt,
            0
          ).toInstant(ZoneOffset.UTC)
    }

    DateTimeFormatter
      .ofPattern(TimeFormatting.hourlyFormat.take(partition.length))
      .withZone(ZoneId.of("UTC"))
      .format(instant.minus(finiteOffset.toJava))
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
  ): IO[List[TablePartition]] = {
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
          TablePartition(value, partitioningFieldTypeFunction)
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
  ): Option[String] = {

    val partitioningFieldType =
      fields
        .find(f => f.getName == partitioningField)
        .map(f => f.getType.getStandardType)
        .orElse {
          partitioningField match {
            case "_PARTITIONTIME" => Some(StandardSQLTypeName.TIMESTAMP)
            case "_PARTITIONDATE" => Some(StandardSQLTypeName.DATE)
            case _                => None
          }
        }

    partitioningFieldType.collect {
      case StandardSQLTypeName.TIMESTAMP => "PARSE_TIMESTAMP"
      case StandardSQLTypeName.DATE      => "PARSE_DATE"
      case _ =>
        throw new IllegalStateException(
          s"Unsupported type $partitioningFieldType"
        )
    }

  }

}
