import SqlAnnotations.{ id, tableName }
import cats.effect.IO
import cats.implicits._
import cats.data._
import cats._
import doobie.{ Fragment, LogHandler, Transactor }
import doobie._
import doobie.implicits._
import doobie.util.transactor
import magnolia.{ CaseClass, Magnolia, SealedTrait }
trait TableCreator[A] {
  def createTable(tableDescription: EntityDesc)(implicit xa: Transactor[IO]): IO[Either[String, Int]]
}

object TableCreator {
  type Typeclass[T] = TableCreator[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): TableCreator[T] = new TableCreator[T] {
    override def createTable(
      tableDescription: EntityDesc
    )(implicit xa: doobie.Transactor[IO]): IO[Either[String, Int]] = {
      // Find the tablename
      val tableName = SqlUtils.findTableName(ctx)

      // Find the id field
      val idField     = SqlUtils.findIdField(ctx)
      val idFieldName = SqlUtils.findFieldName(idField)

      val tableDesc = tableDescription match {
        case t: TableDescRegular => t
        case other =>
          val errorMessage =
            s"TableDescription for type ${ctx.typeName.short} was expected to be TableDescRegular, but was $other"
          throw new RuntimeException(errorMessage)
      }

      val idColumnDataType = SqlUtils.idTypeToString(tableDesc.idColumn.idValueDesc.idType)

      val idFieldDefinition = s"$idFieldName $idColumnDataType"

      val remainingParams = ctx.parameters.filterNot(_ == idField).map { param =>
        val fieldName         = SqlUtils.findFieldName(param)
        lazy val errorMessage = s"Unable to find description for ${param.label} on class ${ctx.typeName.short}"
        val tableDescriptionForParam = tableDesc.additionalColumns
          .find(_.columnName.name == fieldName)
          .getOrElse(throw new RuntimeException(errorMessage))
        (param, tableDescriptionForParam)
      }

      // Create other definitions
      val fieldDefinitions = remainingParams.map {
        case (param, column) =>
          val columnName = SqlUtils.findFieldName(param)
          column.regularValue match {
            case TableDescRegular(tableName, idColumn, additionalColumns, referencesConstraint, isSubtypeTable) =>
              val columnType           = SqlUtils.idTypeToString(idColumn.idValueDesc.idType)
              val foreignKeyColName    = idColumn.columnName.name
              val foreignKeyDownstream = s"foreign key ($columnName) references ${tableName.name} ($foreignKeyColName)"
              val colDefinition        = s"$columnName $columnType"
              val createChild = param.typeclass.createTable(
                TableDescRegular(tableName, idColumn, additionalColumns, referencesConstraint, isSubtypeTable)
              )
              (Some(colDefinition), Some(foreignKeyDownstream), Some(createChild))
            case TableDescSumType(tableName, idColumn, subType) =>
              val columnType           = SqlUtils.idTypeToString(idColumn.idValueDesc.idType)
              val foreignKeyColName    = idColumn.columnName.name
              val foreignKeyDownstream = s"foreign key ($columnName) references ${tableName.name} ($foreignKeyColName)"
              val colDefinition        = s"$columnName $columnType"
              val createChild          = param.typeclass.createTable(TableDescSumType(tableName, idColumn, subType))
              (Some(colDefinition), Some(foreignKeyDownstream), Some(createChild))
            case t: TableDescSeqType =>
              val prog = param.typeclass.createTable(t)
              (None, None, Some(prog))
            case RegularLeaf(dataType) =>
              val columnType    = SqlUtils.idTypeToString(dataType)
              val colDefinition = s"$columnName $columnType"
              (Some(colDefinition), None, None)
            case other =>
              val errorMessage =
                s"EntityDesc for param ${param.label} of class ${ctx.typeName.short} was expected to be of type TableDescRegular, TableDescSymType or RegularLeft, but was $other"
              throw new RuntimeException(errorMessage)
          }
      }

      val nonIdFieldDefinitions = fieldDefinitions.flatMap(_._1)
      val foreignKeyDefinitions = fieldDefinitions.flatMap(_._2)
      val upstreamPrograms      = fieldDefinitions.flatMap(_._3)
      val referencesConstraint = tableDesc.referencesConstraint.map { constraint =>
        s"foreign key (${constraint.columnName.name}) references ${constraint.foreignTableName.name} (${constraint.foreignColumnName.name})"
      }

      // Primary key
      val primaryKey      = s"primary key (${SqlUtils.findFieldName(idField)})"
      val definitions     = Seq(idFieldDefinition) ++ nonIdFieldDefinitions ++ Seq(primaryKey) ++ foreignKeyDefinitions ++ referencesConstraint.toList
      val tableDefinition = s"create table if not exists ${tableName} (${definitions.mkString(", ")})"

      val createTableProg =
        Fragment.const(tableDefinition).updateWithLogHandler(LogHandler.jdkLogHandler).run.transact(xa)
      for {
        a   <- upstreamPrograms.toList.sequence
        res <- createTableProg
      } yield Right(res)
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): TableCreator[T] = new TableCreator[T] {
    override def createTable(
      tableDescription: EntityDesc
    )(implicit xa: doobie.Transactor[IO]): IO[Either[String, Int]] = {
      // Create base table
      // Find the tablename

      val tableDesc = tableDescription match {
        case t: TableDescSumType => t

        case other =>
          val errorMessage =
            s"TableDescription of type ${ctx.typeName.short} was expected to be of type TableDescSumType, but was of type $other"
          throw new RuntimeException(errorMessage)
      }

      val tableName = tableDesc.tableName.name

      // Find the id field name
      val idFieldName = tableDesc.idColumn.columnName.name
      val idFieldType = SqlUtils.idTypeToString(tableDesc.idColumn.idValueDesc.idType)

      val sql      = s"create table if not exists $tableName ($idFieldName $idFieldType, primary key ($idFieldName) )"
      val fragment = Fragment.const(sql)

      // Create subtype tables
      val pairs = ctx.subtypes.map { subType =>
        val tableName  = SqlUtils.findTableName(subType)
        lazy val error = s"Expected to find a subtable for ${subType.typeName.short} but was unable to find one."
        val table = tableDesc.subType
          .find(table => table.tableName.name == tableName)
          .getOrElse(throw new RuntimeException(error))
        (subType, table)
      }

      val subTablePrograms = pairs.map {
        case (subType, subTypeTableDesciption) =>
          subType.typeclass.createTable(subTypeTableDesciption)
      }.toList

      for {
        baseTableRes <- fragment.updateWithLogHandler(LogHandler.jdkLogHandler).run.transact(xa)
        _            <- subTablePrograms.sequence
      } yield Right(baseTableRes)
    }
  }

  implicit val intCreator = new TableCreator[Int] {
    override def createTable(
      tableDescription: EntityDesc
    )(implicit xa: doobie.Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))
  }

  implicit val strCreator = new TableCreator[String] {
    override def createTable(
      tableDescription: EntityDesc
    )(implicit xa: doobie.Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))
  }

  implicit val doubleCreator = new TableCreator[Double] {
    override def createTable(
      tableDescription: EntityDesc
    )(implicit xa: doobie.Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))
  }

  implicit def listCreator[A](implicit creator: TableCreator[A]): TableCreator[List[A]] = new Typeclass[List[A]] {
    override def createTable(tableDescription: EntityDesc)(
      implicit xa: Transactor[IO]
    ): IO[Either[String, Int]] = {
      // create this tableK
      println(s"LIST CREATOR")
      val desc = tableDescription match {
        case t: TableDescSeqType => t
        case other =>
          val errorMessage =
            s"table description for List of something was expected to be TableDescSeqType, but was $other"
          throw new RuntimeException(errorMessage)
      }
      val tableName = desc.tableName.name
      val idColName = desc.idColumn.columnName.name
      val idColType =
        SqlUtils.idTypeToString(SqlUtils.convertToNonAutoIncrementIfPossible(desc.idColumn.idValueDesc.idType))
      val (downStreamColName, downStreamColType) = desc.entityDesc match {
        case TableDescRegular(tableName, idColumn, additionalColumns, referencesConstraint, isSubtypeTable) =>
          (Some(idColumn.columnName.name), idColumn.idValueDesc.idType)
        case TableDescSumType(tableName, idColumn, subType) =>
          (Some(idColumn.columnName.name), idColumn.idValueDesc.idType)
        case TableDescSeqType(tableName, idColumn, entityDesc) =>
          (Some(idColumn.columnName.name), idColumn.idValueDesc.idType)
        case IdLeaf(idValueDesc) =>
          (None, idValueDesc.idType)
        case RegularLeaf(dataType) =>
          (None, dataType)
      }
      val actualColName = downStreamColName.getOrElse("value")
      val actualColType = SqlUtils.idTypeToString(SqlUtils.convertToNonAutoIncrementIfPossible(downStreamColType))
      val sql =
        s"""create table if not exists ${tableName} (
           |$idColName $idColType,
           |$actualColName $actualColType
           |)
         """.stripMargin
      val childProg = creator.createTable(desc.entityDesc)
      val prog      = Fragment.const(sql).updateWithLogHandler(LogHandler.jdkLogHandler).run
      // Recurse
      for {
        res <- childProg
        _   <- prog.transact(xa)
      } yield res
    }
  }

  implicit def gen[T]: TableCreator[T] = macro Magnolia.gen[T]
}

object Test extends App {
  implicit val (url, xa) = PostgresStuff.go()

  case class A(@id a: String, b: Int, c: Double)
  case class B(a: A, @id d: String)
  case class C(@id e: Int, b: B)

  val describer   = TableDescriber.gen[C]
  val description = describer.describe(false, false, TableName(""), null)

  println(description)

  val listTablesProg = Fragment
    .const(s"SELECT table_name FROM information_schema.tables ORDER BY table_name;")
    .query[String]
    .to[List]

  val creator = TableCreator.gen[C]
  val prog = for {
    tablesBefore <- listTablesProg.transact(xa)
    result       <- creator.createTable(description)
    tablesAfter  <- listTablesProg.transact(xa)
  } yield (tablesBefore, result, tablesAfter)

  val (before, res, after) = prog.unsafeRunSync()
  println(before.size)
  println(after.size)
  println(after.diff(before))

  case class Book(@id bookId: Int, name: String, published: Int)

  @tableName("employees") sealed trait Employee
  @tableName("janitors") case class Janitor(@id id: Int, name: String, age: Int) extends Employee
  @tableName("accountants") case class Accountant(@id id: Int, name: String, salary: Double, books: List[Book])
      extends Employee

  val describer2   = TableDescriber.gen[Employee]
  val description2 = describer2.describe(false, false, TableName(""), null)

  val creator2 = TableCreator.gen[Employee]

  val prog2 = for {
    result      <- creator2.createTable(description2)
    tablesAfter <- listTablesProg.transact(xa)
  } yield (result, tablesAfter)

  val (res2, after2) = prog2.unsafeRunSync()
  println(res2)
  println(after2.diff(after))
}
