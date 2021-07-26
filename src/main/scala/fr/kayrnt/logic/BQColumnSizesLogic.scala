package fr.kayrnt.logic

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, Dataset, DatasetId, Table, TableId}
import fr.kayrnt.BQColumnSizesOpts
import fr.kayrnt.model.{ColumnSize, JobLevel, JobPartition}
import fr.kayrnt.reader.{Partitions, Tables}
import fr.kayrnt.writer.OutputWriter
import fr.kayrnt.writer.impl.{BigQueryOutputWriter, CsvOutputWriter}

import scala.jdk.CollectionConverters._

object BQColumnSizesLogic {

  def run(options: BQColumnSizesOpts): Unit = {

    val concurrentQueriesSemaphore: IO[Semaphore[IO]] =
      Semaphore[IO](options.maxConcurrentQueries.map(_.toLong).getOrElse(4))
    val outputFilePath = options.outputFilePath.getOrElse("size.csv")

    def withResources(
        writer: OutputWriter
    )(body: (Semaphore[IO], ColumnSize => IO[_]) => IO[_]): IO[_] =
      concurrentQueriesSemaphore.flatMap { queriesSem =>
        writer.client.use { c =>
          body(queriesSem, c.write).map { _ =>
            c.close()
          }
        }
      }

    val bq: BigQuery = BigQueryOptions
      .newBuilder()
      .setCredentials(GoogleCredentials.getApplicationDefault)
      .setProjectId(options.executionProjectId.getOrElse(options.projectId))
      .build()
      .getService

    val outputWriter = options.outputWriter.getOrElse("csv")

    val outputProject = options.outputProject.getOrElse(options.projectId)

    val outputDataset = options.outputDataset.getOrElse {
      if (outputWriter == "bq")
        throw new IllegalArgumentException("BQ output dataset expected with BQ output")
      else ""
    }

    val outputTable = options.outputTable.getOrElse {
      if (outputWriter == "bq")
        throw new IllegalArgumentException("BQ output table expected with BQ output")
      else ""
    }

    val partition = options.offset.map(o => scala.concurrent.duration.Duration(o)) match {
      case Some(offset) => options.partition.map(p => Partitions.applyOffset(p, offset))
      case None         => options.partition
    }

    val jobFrequency =
      JobLevel.jobLevel(options.jobFrequency.getOrElse("day"))

    val jobPartition = options.partition
      .map(p => JobPartition(p, jobFrequency))
      .getOrElse(JobPartition(jobFrequency))

    val writeDisposition = options.writeDisposition
      .map(w => WriteDisposition.valueOf(w.toUpperCase))
      .getOrElse(WriteDisposition.WRITE_TRUNCATE)

    val outputWriterImpl = outputWriter match {
      case "bq" =>
        new BigQueryOutputWriter(
          bq,
          outputProject,
          outputDataset,
          outputTable,
          outputFilePath,
          partition,
          jobPartition,
          writeDisposition
        )
      case _ => new CsvOutputWriter(outputFilePath, writeDisposition)
    }

    def datasets = options.dataset match {
      case None => bq.listDatasets(options.projectId).iterateAll().asScala
      case Some(datasetStr) =>
        val datasets = datasetStr.split(",").toList
        for {
          dataset <- datasets
        } yield bq.getDataset(DatasetId.of(options.projectId, dataset))
    }

    def tables = options.table match {
      case None => datasets.flatMap(ds => bq.listTables(ds.getDatasetId).iterateAll().asScala)
      case Some(tableStr) =>
        val tables = tableStr.split(",").toList
        for {
          dataset <- datasets
          table   <- tables
        } yield bq.getTable(TableId.of(options.projectId, dataset.getDatasetId.getDataset, table))
    }

    if (options.table.isDefined && options.dataset.isEmpty)
      throw new IllegalArgumentException(
        s"Expected dataset details provided but option wasn't provided"
      )

    withResources(outputWriterImpl) { (sem, csvWriter) =>
      (for {
        table <- tables
      } yield Tables.analyzeTable(
        bq,
        sem,
        options.projectId,
        table,
        csvWriter,
        partition
      )).toList.sequence
    }.unsafeRunSync()

  }

}
