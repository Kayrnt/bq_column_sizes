package fr.kayrnt.model

case class ColumnSize(
    projectId: String,
    dataset: String,
    table: String,
    field: String,
    partition: String,
    sizeInBytes: Long
) {
  def toList: List[String] = List(projectId, dataset, table, field, partition, sizeInBytes.toString)
}
