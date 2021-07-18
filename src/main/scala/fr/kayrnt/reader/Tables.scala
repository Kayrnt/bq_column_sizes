package fr.kayrnt.reader

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.implicits._
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.model.{ColumnSize, JobPartition}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.util.{Success, Try}

object Tables extends LazyLogging {

  def analyzeTable(
      bq: BigQuery,
      sem: Semaphore[IO],
      projectId: String,
      table: Table,
      rowWriter: ColumnSize => IO[_],
      partition: scala.Option[String]
  ): IO[Unit] = try {
    val definition: TableDefinition = table.getDefinition
    definition match {
      case std: StandardTableDefinition =>
        analyzeTableDefinition(bq, sem, projectId, table, std, rowWriter, partition)
      case _ =>
        throw new IllegalStateException(
          s"Unsupported table definition $definition"
        )
    }
  } catch {
    case NonFatal(e) =>
      throw new IllegalStateException(
        s"Couldn't retrieve definition for table $table. Are you sure that this table exists?",
        e
      )
  }

  def analyzeTableDefinition(
      bq: BigQuery,
      sem: Semaphore[IO],
      projectId: String,
      table: Table,
      std: StandardTableDefinition,
      rowWriter: ColumnSize => IO[_],
      partition: scala.Option[String]
  ): IO[Unit] = {
    val schema                       = std.getSchema()
    val partitioningFieldRaw: String = std.getTimePartitioning.getField
    val partitioningField: String =
      if (partitioningFieldRaw == null) "_PARTITIONTIME" else partitioningFieldRaw
    val partitioningType: TimePartitioning.Type = std.getTimePartitioning.getType

    val dataset  = table.getTableId.getDataset
    val tableStr = table.getTableId.getTable

    val tableReference =
      table.getTableId.getProject + "." +
        table.getTableId.getDataset + "." +
        table.getTableId.getTable

    logger.info(s"Analyzing $tableReference...")

    val fields: Seq[Field] = schema.getFields.iterator().asScala.toList

    val partitioningFieldTypeFunction: String =
      Partitions
        .getPartitioningFieldTypeFunction(fields, partitioningField)
        .getOrElse(
          throw new IllegalStateException(
            s"Unable to find type for partitioning field ($partitioningField) / partitioning type ($partitioningType)"
          )
        )

    val partitions = Partitions
      .getPartitions(bq, sem, tableReference, partitioningFieldTypeFunction, partition)

    partitions
      .flatMap { partitions =>
        partitions.flatMap { partition =>
          fields.map { f =>
            val fieldName = f.getName

            val query =
              s"""|#standardSQL
                  |SELECT `$fieldName`
                  |FROM `$tableReference`
                  |WHERE $partitioningField = ${partition.toQueryCondition}
                  |""".stripMargin
            getQueryBytes(
              bq,
              query
            ).flatMap { sizeInBytes =>
              val cs = ColumnSize(
                partition.formattedForOutput,
                projectId,
                dataset,
                tableStr,
                fieldName,
                partition.formattedForOutput,
                sizeInBytes
              )
              rowWriter(cs)
            }
          }
        }.sequence
      }.map(_ => ())
  }

  def getQueryBytes(bq: BigQuery, queryString: String): IO[Long] = {
    logger.debug("getQueryBytes: " + queryString)
    val config = QueryJobConfiguration
      .newBuilder(queryString)
      .setDryRun(true)
      .setUseQueryCache(false)
      .build()
    IO {
      val job = bq.create(JobInfo.of(config))
      val statistics: JobStatistics.QueryStatistics =
        job.getStatistics()
      Long.unbox(statistics.getTotalBytesProcessed())
    }
  }

}
