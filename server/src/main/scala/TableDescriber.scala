import SqlAnnotations.{ id, tableName }
import magnolia.{ CaseClass, Magnolia, SealedTrait }

trait TableDescriber[A] {
  def describe(isId: Boolean, isSubtypeTable: Boolean, assignedTableName: TableName, parentIdColumn: IdColumn): EntityDesc
}

object TableDescriber {
  type Typeclass[T] = TableDescriber[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): TableDescriber[T] = (isId, isSubtypeTable, parentTableName, parentIdCol) => {
    val tableName = SqlUtils.findTableName(ctx)
    val idField = SqlUtils.findIdField(ctx)
    val idFieldName = SqlUtils.findFieldName(idField)
    val remaining = ctx.parameters.filterNot(_ == idField)

    val idColDataType = idField.typeclass.describe(!isSubtypeTable, false, TableName(tableName), null) match {
      case IdLeaf(idValueDesc) => idValueDesc
      case RegularLeaf(dataType) => IdValueDesc(SqlUtils.narrowToIdDataData(dataType))
      case other =>
        val errorMessage = s"Id column of type ${ctx.typeName.short} was expected to be of type IdLeaf, but was $other"
        throw new RuntimeException(errorMessage)
    }
    val idFieldStructure = IdColumn(ColumnName(idFieldName), idColDataType)

    val nonIdFieldStructure = remaining.map { param =>
      val fieldName = SqlUtils.findFieldName(param)
      val fieldDesc = param.typeclass.describe(false, false, TableName(s"${tableName}_$fieldName"), idFieldStructure)
      RegularColumn(ColumnName(fieldName), fieldDesc)
    }.toList

    TableDescRegular(TableName(tableName), idFieldStructure, nonIdFieldStructure, None, isSubtypeTable)
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): TableDescriber[T] = (isId, isSubtypeTable, parentTableName, parentIdCol) => {
    val baseTableName = TableName(SqlUtils.findTableName(ctx))
    val idFieldName = ColumnName(s"${baseTableName.name}_id")

    val subTypeDesciptions = ctx.subtypes.map { subType =>
      val subTable = subType.typeclass.describe(false, true, baseTableName, null)
      val subTableDesc = subTable match {
        case TableDescRegular(tableName, idColumn, additionalColumns, _, _) =>
          TableDescRegular(
            tableName,
            idColumn,
            additionalColumns,
            Some(ReferencesConstraint(idColumn.columnName, baseTableName, idFieldName)),
            true)
        case other =>
          val errorMessage =
            s"Subtype ${subType.typeName.short} of sealed trait ${ctx.typeName.short} was expected to generate TableDescRegular, but was $other"
          throw new RuntimeException(errorMessage)
      }
      subTableDesc
    }
    val idColDataType = SqlUtils.narrowToAutoIncrementIfPossible(subTypeDesciptions.head.idColumn.idValueDesc.idType)
    val idCol = IdColumn(idFieldName, IdValueDesc(idColDataType))

    TableDescSumType(baseTableName, idCol, subTypeDesciptions)
  }

  implicit val intUpdater: TableDescriber[Int] = (isId, isSubtypeTable, parentTableName, parentIdCol) =>
    if (isId) IdLeaf(IdValueDesc(Serial)) else RegularLeaf(Integer)

  implicit val stringUpdater: TableDescriber[String] = (isId, isSubtypeTable, parentTableName, parentIdCol) =>
    if (isId) IdLeaf(IdValueDesc(Character(10))) else RegularLeaf(Text)

  implicit val doubleUpdater: TableDescriber[Double] = (isId, isSubtypeTable, parentTableName, parentIdCol) =>
    if (isId) IdLeaf(IdValueDesc(Serial)) else RegularLeaf(Float)

  implicit def listDesc[A](implicit describer: TableDescriber[A]): TableDescriber[List[A]] = new Typeclass[List[A]] {
    override def describe(isId: Boolean, isSubtypeTable: Boolean, parentTableName: TableName, parentIdCol: IdColumn): EntityDesc = {
      val subType = describer.describe(false, false, TableName(""), null)
      val tableName = parentTableName
      TableDescSeqType(tableName, parentIdCol, subType)
    }
  }

  implicit def gen[T]: TableDescriber[T] = macro Magnolia.gen[T]
}

object Desc extends App {
  @tableName("employees") sealed trait Employee
  @tableName("janitors") case class Janitor(@id id: Int, name: String, age: Int) extends Employee
  @tableName("accountants") case class Accountant(@id id: Int, name: String, salary: Double) extends Employee

  case class A(@id a: String, b: Int, c: Double)
  case class B(a: A, @id d: String)
  case class C(@id e: Int, b: B)

  //  val desc = TableDescriber.gen[Cabin]
  val desc2 = TableDescriber.gen[Employee]
  val desc3 = TableDescriber.gen[C]

  //  println(desc.describe())
  //  val desc = desc2.describe(false, false).asInstanceOf[TableDescSumType]
  //  println(desc.idColumn)
  //  println(desc.tableName)
  //  desc.subType.foreach(println)
  //  val desc4 = desc3.describe(false, false).asInstanceOf[TableDescRegular]
  //  println(desc4)
  //  println()
  //  desc4.additionalColumns.foreach(println)
  //  println()

  case class D(@id a: Int, names: List[Employee], muf: C)

  val descList = implicitly[TableDescriber[D]]
  val desc = descList.describe(false, false, TableName(""), null).asInstanceOf[TableDescRegular]
  println(desc.idColumn)
  println(desc.additionalColumns.head)
  println(desc.additionalColumns.tail.head)

}
