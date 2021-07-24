package fr.kayrnt.writer.impl

import cats.effect.{IO, Resource}
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.writer.OutputWriterClient
import com.google.cloud.bigquery.{BigQuery, BigQueryException, CsvOptions, Field, JobId, JobInfo, Schema, StandardSQLTypeName, TableId, TimePartitioning, WriteChannelConfiguration}
import fr.kayrnt.model.{ColumnSize, JobPartition}
import fr.kayrnt.reader.Partitions

import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.Path
import scala.util.chaining.scalaUtilChainingOps
import scala.jdk.CollectionConverters._

class BigQueryOutputWriter(
    bq: BigQuery,
    outputProjectName: String,
    outputDatasetName: String,
    outputTableName: String,
    outputFilePath: String,
    partitionOpt: Option[String],
    jobPartition: JobPartition,
    writeDisposition: JobInfo.WriteDisposition
) extends CsvOutputWriter(outputFilePath, WriteDisposition.WRITE_TRUNCATE)
    with LazyLogging {

  val fields = List(
    Field.of("job_partition", StandardSQLTypeName.TIMESTAMP),
    Field.of("project_id", StandardSQLTypeName.STRING),
    Field.of("dataset", StandardSQLTypeName.STRING),
    Field.of("table", StandardSQLTypeName.STRING),
    Field.of("field", StandardSQLTypeName.STRING),
    Field.of("table_partition", StandardSQLTypeName.TIMESTAMP),
    Field.of("size_in_bytes", StandardSQLTypeName.INT64)
  )

  val schema: Schema = Schema.of(fields: _*)

  val partitioning: TimePartitioning =
    TimePartitioning
      .newBuilder(jobPartition.timePartitioning)
      .setField("job_partition")
      .build()

  override val client: Resource[IO, OutputWriterClient] =
    csvClient.map { csvWriterClient =>
      logger.info("Preparing BQ output writer client...")
      new OutputWriterClient {
        override def write(columnSize: ColumnSize): IO[_] =
          csvWriterClient.write(columnSize)

        override def close(): Unit = {
          csvWriterClient.close()
          val outputTableNameWithPartition =
            partitionOpt.map(p => s"$outputTableName$$$p").getOrElse(outputTableName)
          logger.info(s"Writing output to $outputDatasetName.$outputTableNameWithPartition")
          try {
            // Skip header row in the file.
            val csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build()

            val tableId =
              TableId.of(outputProjectName, outputDatasetName, outputTableNameWithPartition)
            val writeChannelConfiguration = WriteChannelConfiguration
              .newBuilder(tableId)
              .setSchema(schema)
              .setWriteDisposition(writeDisposition)
              .setFormatOptions(csvOptions)
              .pipe(_.setTimePartitioning(partitioning))
              .build

            val jobId  = JobId.newBuilder.setLocation("US").build
            val writer = bq.writer(jobId, writeChannelConfiguration)

            // Write data to writer
            val stream  = Channels.newOutputStream(writer)
            val csvPath = Path.of(outputFilePath)
            try Files.copy(csvPath, stream)
            finally if (stream != null) stream.close()
            // Get load job
            val job       = writer.getJob
            val waitedJob = job.waitFor()
            if (waitedJob.isDone()) {
              logger.info("CSV successfully added during load append job")
            } else {
              logger.error(
                "BigQuery was unable to load into the table due to an error:"
                  + waitedJob.getStatus().getError()
              )
            }
          } catch {
            case e: Exception
                if e.isInstanceOf[BigQueryException] || e.isInstanceOf[InterruptedException] =>
              logger.info("Column not added during load append \n" + e.toString)
          }
        }

      }
    }

}
