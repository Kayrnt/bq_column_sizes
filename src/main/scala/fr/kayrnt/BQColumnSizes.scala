package fr.kayrnt

import caseapp.core.app.CaseApp
import caseapp.{AppName, AppVersion, ExtraName, HelpMessage, ProgName, RemainingArgs}
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.logic.BQColumnSizesLogic

@AppName("BQColumnSizes")
@AppVersion("1.0.0")
@ProgName("bq-column-sizes")
case class BQColumnSizesOpts(
    @ExtraName("ep")
    @HelpMessage("GCP project id used for query execution if different from read.")
    executionProjectId: Option[String],
    @ExtraName("p")
    @HelpMessage(
      "GCP project id to analyze, if no dataset is provided, it will analyze all datasets for that project."
    )
    projectId: String,
    @ExtraName("d")
    @HelpMessage(
      "dataset to analyze, if no table is provided it will analyze all tables for that dataset."
    )
    dataset: Option[String],
    @ExtraName("t")
    @HelpMessage(
      "table to analyze"
    )
    table: Option[String],
    @ExtraName("o")
    @HelpMessage(
      "output file path, default in size.csv, you can use an absolute path such as /Users/myUser/size.csv."
    )
    outputFilePath: Option[String],
    @ExtraName("mcq")
    @HelpMessage(
      "max concurrent queries as you're throttled on actual BQ requests, default to 4."
    )
    maxConcurrentQueries: Option[Int],
    @ExtraName("ow")
    @HelpMessage(
      "output writer such as bq or csv, default to csv"
    )
    outputWriter: Option[String],
    @ExtraName("op")
    @HelpMessage(
      "Bigquery output project when bq output type is selected, default to the projectId field."
    )
    outputProject: Option[String],
    @ExtraName("od")
    @HelpMessage(
      "Bigquery output dataset when bq output type is selected"
    )
    outputDataset: Option[String],
    @ExtraName("ot")
    @HelpMessage(
      "Bigquery output table when bq output type is selected"
    )
    outputTable: Option[String],
    @ExtraName("pt")
    @HelpMessage(
      """Partition to analyze, if not specified all partitions will be analyzed.
        |Format: yyyy, yyyyMM, yyyyMMdd, yyyyyMMddHH
        |Example: 20210101""".stripMargin
    )
    partition: Option[String],
    @HelpMessage(
      """Partition offset as duration
        |Format: <length><unit> based on scala.concurrent.duration.Duration
        |Example: 1hour, 1d""".stripMargin
    )
    offset: Option[String],
)

object BQColumnSizes extends CaseApp[BQColumnSizesOpts] with LazyLogging {
  override val ignoreUnrecognized: Boolean = true

  def run(options: BQColumnSizesOpts, arg: RemainingArgs): Unit = {

    logger.info("options :" + options)
    logger.info("unhandled args :" + arg.toString())

    BQColumnSizesLogic.run(options)

    logger.info("Exiting...")
  }

}
