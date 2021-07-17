package fr.kayrnt.model

case class ColumnSize(
    job_partition: String,
    project_id: String,
    dataset: String,
    table: String,
    field: String,
    table_partition: String,
    size_in_bytes: Long
) {
  def toList: List[String] =
    List(job_partition, project_id, dataset, table, field, table_partition, size_in_bytes.toString)
}
