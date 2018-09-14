import SqlAnnotations.{ fieldName, id, tableName }
import cats.effect.{ Effect, IO }
import doobie.{ Fragment, LogHandler, Transactor }
import magnolia.{ CaseClass, Magnolia, SealedTrait }
import cats.Traverse
import cats.effect.IO
import doobie.{ Fragment, LogHandler, Transactor }
import magnolia.{ CaseClass, Magnolia, SealedTrait }
import cats.effect.IO
import doobie.{ Fragment, LogHandler, Transactor }
import magnolia._
import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import com.danielasfregola.randomdatagenerator.magnolia.RandomDataGenerator._
import doobie._
import doobie.implicits._
import doobie.util.transactor
import org.scalacheck.Arbitrary
import scalaprops.Gen
import scalaprops.Shapeless._

import scala.concurrent.duration._
import scala.util.Try
trait Repo[A, B] {
  def findById(id: A): IO[Option[B]]
  def createTables(): IO[Unit]
  def insert(value: B): IO[A]
}

trait IdTransformer[A] {
  def fromString(str: String): A
}

class RepoFromOps[A, B](repoOps: RepoOps[B])(implicit val transactor: Transactor[IO], idTransformer: IdTransformer[A])
    extends Repo[A, B] {
  val desc = repoOps.describe(false, false, TableName(""), null)

  override def createTables(): IO[Unit] =
    for {
      _ <- repoOps.createTable(desc)
    } yield ()

  override def insert(value: B): IO[A] =
    for {
      res <- repoOps.save(value, desc, None)
      _   = println(res)
    } yield {
      idTransformer.fromString(res.right.get)
    }

  override def findById(id: A): IO[Option[B]] =
    repoOps.findById(id.toString, desc)
}

trait RepoOps[A] {
  def findById(id: String, tableDescription: EntityDesc)(implicit xa: Transactor[IO]): IO[Option[A]]
  def save(value: A, tableDescription: EntityDesc, assignedId: Option[String] = None)(
    implicit
    xa: Transactor[IO]
  ): IO[Either[String, String]]
  def createTable(tableDescription: EntityDesc)(implicit xa: Transactor[IO]): IO[Either[String, Int]]
  def describe(isId: Boolean,
               isSubtypeTable: Boolean,
               assignedTableName: TableName,
               parentIdColumn: IdColumn): EntityDesc
}

object RepoOps {
  type Typeclass[T] = RepoOps[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): RepoOps[T] = new RepoOps[T] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[T]] = {
      val tableName   = SqlUtils.findTableName(ctx)
      val idField     = SqlUtils.findIdField(ctx)
      val idFieldName = SqlUtils.findFieldName(idField)

      val tableDesc = tableDescription match {
        case t: TableDescRegular => t
        case other =>
          val errorMessage =
            s"Tabledescription for type ${ctx.typeName.short} was expected to be of type TableDescRegular, but was $other"
          throw new RuntimeException(errorMessage)
      }

      val subPrograms: IO[List[Option[Any]]] = ctx.parameters.toList.traverse { param =>
        val (_, descForParam) = SqlUtils.entityDescForParam(param, tableDesc, ctx)
        val isSeqType = descForParam match {
          case TableDescSeqType(_, _, _) => true
          case _                         => false
        }

        if (isSeqType) {
          param.typeclass.findById(id, descForParam)
        } else {
          val fieldName = SqlUtils.findFieldName(param)
          val sqlStr    = s"select $fieldName from $tableName where $idFieldName = '$id'"
          val queryResult = Fragment
            .const(sqlStr)
            .queryWithLogHandler[String](LogHandler.jdkLogHandler)
            .to[List]
            .transact(xa)
            .map(_.headOption)

          val res = queryResult.flatMap { maybeString =>
            maybeString
              .map(str => param.typeclass.findById(str, descForParam))
              .getOrElse(IO(None))
          }
          res
        }

      }

      for {
        subResults <- subPrograms
      } yield Try(Some(ctx.rawConstruct(subResults.map(_.get)))).getOrElse(None)
    }

    override def save(value: T, tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] = {
      // Three cases
      // - Subtype table, id is provided
      // - Regular table with auto fill id, dont insert id
      // - Regular table without autofil id, insert id
      // TODO: Cases when idtype is an option

      // Find the tablename
      val tableName = SqlUtils.findTableName(ctx)

      // Find the id field
      val idField = SqlUtils.findIdField(ctx)

      val tableDesc = tableDescription match {
        case t: TableDescRegular => t
        case other =>
          val errorMessage =
            s"TableDescription for type ${ctx.typeName.short} was expected to be of type TableDescRegular, but was $other"
          throw new RuntimeException(errorMessage)
      }

      val (pars, (idLable, idValue)) =
        (tableDesc.isSubtypeTable, assignedId, tableDesc.idColumn.idValueDesc.idType) match {
          case (true, Some(id), _) =>
            // Id is provided, dont process the id param
            (ctx.parameters.filterNot(_ == idField), (Seq(SqlUtils.findFieldName(idField)), Seq(Right(id))))
          case (true, None, _) =>
            val errorMessage =
              s"Tabledescription for type ${ctx.typeName.short} specifies a subtype table, but no id was provided."
            throw new RuntimeException(errorMessage)
          case (false, _, idType) if SqlUtils.isAutofillFieldType(idType) =>
            // Id will be auto filled by database, dont process the id param
            (ctx.parameters.filterNot(_ == idField), (Seq(), Seq()))
          case _ =>
            (ctx.parameters, (Seq(), Seq()))
        }

      val (paramsToInsert, seqParams) = pars
        .map(param => SqlUtils.entityDescForParam(param, tableDesc, ctx))
        .partition {
          case (_, entityDesc) =>
            entityDesc match {
              case TableDescSeqType(_, _, _) => false
              case _                         => true
            }
        }

      val labels = idLable ++ paramsToInsert.map { case (param, _) => SqlUtils.findFieldName(param) }
      val values: IO[List[Either[String, String]]] = paramsToInsert.toList.traverse {
        case (param, tDesc) => param.typeclass.save(param.dereference(value), tDesc)
      }

      for {
        idValues <- values
        updates = (idValue ++ idValues)
          .foldLeft[List[String]](Nil) { (acc, next) =>
            next match {
              case Right(v) => v :: acc
              case Left(v)  => throw new RuntimeException(s"Error while inserting ${value}: $v")
            }
          }
          .reverse
        columnDefinitions = labels.mkString("(", ", ", ")")
        valueDefinitions  = updates.map(str => s"'$str'").mkString("(", ", ", ")")
        update = Fragment.const("""insert into """) ++
          Fragment.const(tableName) ++
          Fragment.const(columnDefinitions) ++
          Fragment.const(" values ") ++
          Fragment.const(valueDefinitions)

        _ = println(update.toString())
        response <- update
                     .updateWithLogHandler(LogHandler.jdkLogHandler)
                     .withUniqueGeneratedKeys[String](SqlUtils.findFieldName(idField))
                     .transact(xa)
        _ <- seqParams.toList.traverse {
              case (oaram, paramDesc) => oaram.typeclass.save(oaram.dereference(value), paramDesc, Some(response))
            }
      } yield Right(response)

    }

    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] = {
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

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc = {
      val tableName   = SqlUtils.findTableName(ctx)
      val idField     = SqlUtils.findIdField(ctx)
      val idFieldName = SqlUtils.findFieldName(idField)
      val remaining   = ctx.parameters.filterNot(_ == idField)

      val idColDataType = idField.typeclass.describe(!isSubtypeTable, false, TableName(tableName), null) match {
        case IdLeaf(idValueDesc)   => idValueDesc
        case RegularLeaf(dataType) => IdValueDesc(SqlUtils.narrowToIdDataData(dataType))
        case other =>
          val errorMessage =
            s"Id column of type ${ctx.typeName.short} was expected to be of type IdLeaf, but was $other"
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
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): RepoOps[T] = new RepoOps[T] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[T]] = {
      val tableDesc = tableDescription match {
        case t: TableDescSumType => t
        case _                   => throw new RuntimeException("ms")
      }
      val res = ctx.subtypes.map { subType =>
        val subTable = SqlUtils.entityDescForSubtype(subType, tableDesc, ctx)
        subType.typeclass
          .findById(id, subTable)
          .map(_.map { stuff =>
            val t = subType.cast(stuff)
            t
          })
      }
      val meh: IO[List[Option[Any]]] = res.toList.sequence

      meh.map(
        list =>
          list.collectFirst {
            case Some(a) => a.asInstanceOf[T]
        }
      )
    }

    override def save(value: T, tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] = {
      // Insert in base table
      val tableDesc = tableDescription match {
        case t: TableDescSumType => t
        case other =>
          val errorMessage =
            s"Tabledescription for type ${ctx.typeName.short} was expected to be of type TableDescSumType, but was $other"
          throw new RuntimeException(errorMessage)
      }
      val baseTableName = tableDesc.tableName.name
      val isAutofillId  = SqlUtils.isAutofillFieldType(tableDesc.idColumn.idValueDesc.idType)
      val colName       = tableDesc.idColumn.columnName
      val sql =
        if (isAutofillId)
          s"insert into $baseTableName (${colName.name}) values (DEFAULT)"
        else {
          val generatedId = java.util.UUID.randomUUID().toString // TODO: Fix
          s"insert into $baseTableName (${colName.name}) values ('$generatedId')"
        }

      println(sql)

      val insertProg = Fragment
        .const(sql)
        .updateWithLogHandler(LogHandler.jdkLogHandler)
        .withUniqueGeneratedKeys[String](colName.name)
        .transact(xa)

      for {
        id <- insertProg
        resp <- ctx.dispatch(value)(
                 sub => sub.typeclass.save(sub cast value, SqlUtils.entityDescForSubtype(sub, tableDesc, ctx), Some(id))
               )
      } yield resp

    }

    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] = {
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

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc = {
      val baseTableName = TableName(SqlUtils.findTableName(ctx))
      val idFieldName   = ColumnName(s"${baseTableName.name}_id")

      val subTypeDesciptions = ctx.subtypes.map { subType =>
        val subTable = subType.typeclass.describe(false, true, baseTableName, null)
        val subTableDesc = subTable match {
          case TableDescRegular(tableName, idColumn, additionalColumns, _, _) =>
            TableDescRegular(tableName,
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
      val idCol         = IdColumn(idFieldName, IdValueDesc(idColDataType))

      TableDescSumType(baseTableName, idCol, subTypeDesciptions)
    }
  }

  implicit def gen[T]: RepoOps[T] = macro Magnolia.gen[T]

  implicit val intUpdater: RepoOps[Int] = new RepoOps[Int] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[Int]] =
      IO(Some(id.toInt))

    override def save(value: Int, tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value.toString))

    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc =
      if (isId) IdLeaf(IdValueDesc(Serial)) else RegularLeaf(Integer)
  }

  implicit val stringUpdater: RepoOps[String] = new RepoOps[String] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[String]] =
      IO(Some(id))

    override def save(value: String, tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value.toString))

    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc =
      if (isId) IdLeaf(IdValueDesc(Character(40))) else RegularLeaf(Text)
  }

  implicit val doubleUpdater: RepoOps[Double] = new RepoOps[Double] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[Double]] =
      IO(Some(id.toDouble))

    override def save(value: Double, tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value.toString))

    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] =
      IO(Right(0))

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc =
      if (isId) IdLeaf(IdValueDesc(Serial)) else RegularLeaf(Float)
  }

  implicit def listOps[A](implicit ops: RepoOps[A]) = new Typeclass[List[A]] {
    override def createTable(tableDescription: EntityDesc)(implicit
                                                           xa: Transactor[IO]): IO[Either[String, Int]] = {
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
        case TableDescRegular(_, idColumn, additionalColumns, referencesConstraint, isSubtypeTable) =>
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
      val childProg = ops.createTable(desc.entityDesc)
      val prog      = Fragment.const(sql).updateWithLogHandler(LogHandler.jdkLogHandler).run
      // Recurse
      for {
        res <- childProg
        _   <- prog.transact(xa)
      } yield res
    }

    override def describe(isId: Boolean,
                          isSubtypeTable: Boolean,
                          assignedTableName: TableName,
                          parentIdColumn: IdColumn): EntityDesc = {
      val subType   = ops.describe(false, false, TableName(""), null)
      val tableName = assignedTableName
      TableDescSeqType(tableName, parentIdColumn, subType)
    }

    override def save(value: List[A], tableDescription: EntityDesc, assignedId: Option[String])(
      implicit
      xa: Transactor[IO]
    ): IO[Either[String, String]] = {
      // decide if this is a value or an object
      val desc = tableDescription match {
        case t: TableDescSeqType =>
          t
        case other =>
          val errorMessage = s"Table Description for list was expected to be of type TableDescSeqType, but was $other"
          throw new RuntimeException(errorMessage)
      }
      val id        = assignedId.get
      val tableName = desc.tableName.name
      val downstreamColName = desc.entityDesc match {
        case TableDescRegular(_, idColumn, _, _, _) => idColumn.columnName.name
        case TableDescSumType(_, idColumn, _)       => idColumn.columnName.name
        case TableDescSeqType(_, idColumn, _)       => idColumn.columnName.name
        case IdLeaf(_)                              => "value"
        case RegularLeaf(_)                         => "value"
      }
      val upstreamColName = desc.idColumn.columnName.name
      for {
        upstreamResults <- value.traverse(v => ops.save(v, desc.entityDesc, None))
        omg             = upstreamResults.map(_.right.get)
        progs = omg.map { res =>
          val sql = s"""insert into $tableName ($upstreamColName, $downstreamColName) values ('$id', '$res')"""
          println(sql)
          Fragment.const(sql)
        }
        meh <- progs.traverse(prog => prog.updateWithLogHandler(LogHandler.jdkLogHandler).run.transact(xa))
      } yield Right(meh.headOption.map(_.toString).getOrElse(java.util.UUID.randomUUID().toString))

    }

    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: Transactor[IO]): IO[Option[List[A]]] = {
      println(s"LIST FINDER")
      val desc = tableDescription match {
        case t: TableDescSeqType => t
        case other =>
          val errorMessage = s"Entity Desc for list type was expected to be of type TableDescSeqType, but was $other"
          throw new RuntimeException(errorMessage)
      }

      val tableName = desc.tableName.name
      val idColName = desc.idColumn.columnName.name
      val downstreamColName = desc.entityDesc match {
        case TableDescRegular(_, idColumn, _, _, _) => idColumn.columnName.name
        case TableDescSumType(_, idColumn, _)       => idColumn.columnName.name
        case TableDescSeqType(_, idColumn, _)       => idColumn.columnName.name
        case IdLeaf(_)                              => "value"
        case RegularLeaf(_)                         => "value"
      }
      val sql = s"select $downstreamColName from $tableName where $idColName = '$id'"
      val fragment =
        Fragment
          .const(sql)
          .queryWithLogHandler[String](LogHandler.jdkLogHandler)
          .to[List]

      for {
        ids <- fragment.transact(xa)
        res <- ids.traverse(id => ops.findById(id, desc.entityDesc))
      } yield res.sequence
    }
  }

  def toRepo[A, B](repoOps: RepoOps[B])(implicit
                                        transactor: Transactor[IO],
                                        idTransformer: IdTransformer[A]): Repo[A, B] =
    new RepoFromOps(repoOps)
}

object Example extends App {

  case class Book(@id @fieldName("book_id") bookId: Int, title: String, published: Int)
  sealed trait Person
  case class Friend(@id id: Int, name: String, daysFriends: Int, books: List[Book]) extends Person
  case class Stranger(@id id: Int, distance: Double, streetName: List[String])      extends Person

  implicit val idTransformer = new IdTransformer[Int] {
    override def fromString(str: String): Int =
      str.toInt
  }
  implicit val (url, xa) = PostgresStuff.go()

  val personRepo      = RepoOps.toRepo[Int, Person](RepoOps.gen[Person])
  val exampleFriend   = Friend(0, "Rune", 1000, List(Book(1, "The book", 2001)))
  val exampleStranger = Stranger(0, 13.2, List("Snuffy"))

  val prog = for {
    _             <- personRepo.createTables()
    id            <- personRepo.insert(exampleFriend)
    id2           <- personRepo.insert(exampleStranger)
    foundFriend   <- personRepo.findById(id)
    foundStranger <- personRepo.findById(id2)
  } yield (foundFriend, foundStranger)

  val start              = System.nanoTime() nanos;
  val (friend, stranger) = prog.unsafeRunSync()
  val end                = System.nanoTime() nanos;

  println((end - start).toMillis)

  println(friend)
  println(stranger)
}
