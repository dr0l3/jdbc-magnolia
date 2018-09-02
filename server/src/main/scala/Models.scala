sealed trait FieldType
case object UUID extends FieldType
case object Integer extends FieldType
case object Double extends FieldType

case class TableName(name: String) extends AnyVal
case class FieldName(name: String) extends AnyVal

trait FieldReference
case class SimpleValue(fieldType: FieldType) extends FieldReference
case class ObjectReference(fieldType: FieldType, tableName: TableName, fieldName: FieldName) extends FieldReference
case class SumTypeObjectReference() extends FieldReference

case class TableDescription(idField: FieldReference, additionalFields: List[(FieldName, FieldReference)])


