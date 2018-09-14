sealed trait DataType
case object Float extends DataType
case object Text  extends DataType

sealed trait IdType          extends DataType
case object Serial           extends IdType
case object BigSerial        extends IdType
case object Integer          extends IdType
case class Character(n: Int) extends IdType // Must specify size of ids
case object UUID             extends IdType

case class TableName(name: String)  extends AnyVal
case class ColumnName(name: String) extends AnyVal

trait FieldReference
case class SimpleValue(fieldType: DataType)                                                  extends FieldReference
case class ObjectReference(fieldType: DataType, tableName: TableName, fieldName: ColumnName) extends FieldReference
case class SumTypeObjectReference(baseTable: ObjectReference, subTypeTables: List[TableDescription])
    extends FieldReference

case class TableDescription(idField: FieldReference,
                            additionalFields: List[(ColumnName, TableDescription)],
                            referencesConstraint: Option[(TableName, ColumnName)] = None)

case class IdValueDesc(idType: IdType) //isProvided true if subtypetable
case class IdColumn(columnName: ColumnName, idValueDesc: IdValueDesc)

case class RegularColumn(columnName: ColumnName, regularValue: EntityDesc)
case class ReferencesConstraint(columnName: ColumnName, foreignTableName: TableName, foreignColumnName: ColumnName)

sealed trait EntityDesc
case class TableDescRegular(tableName: TableName,
                            idColumn: IdColumn,
                            additionalColumns: Seq[RegularColumn],
                            referencesConstraint: Option[ReferencesConstraint],
                            isSubtypeTable: Boolean)
    extends EntityDesc
case class TableDescSumType(tableName: TableName, idColumn: IdColumn, subType: Seq[TableDescRegular]) extends EntityDesc
case class TableDescSeqType(tableName: TableName, idColumn: IdColumn, entityDesc: EntityDesc)         extends EntityDesc
case class IdLeaf(idValueDesc: IdValueDesc)                                                           extends EntityDesc
case class RegularLeaf(dataType: DataType)                                                            extends EntityDesc
