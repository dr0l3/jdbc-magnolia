import java.nio.file.Paths

import SqlAnnotations._
import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.implicits._
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres._
import magnolia.{Param, _}
import shapeless.tupled._

import scala.concurrent.duration._
import scala.annotation.Annotation
import scala.language.experimental.macros
import java.nio.file.Paths
import java.sql.ResultSet

import cats.effect.IO
import doobie.{LogHandler, Transactor}
import fs2.Stream
import magnolia.{CaseClass, Param, SealedTrait, Subtype}
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres

import scala.collection.mutable.ListBuffer
object SqlUtils {
  def idTypeString[A](a: A): Option[String] =
    a match {
      case Int => Some("SERIAL")
      case _   => None
    }

  def findFieldName[Ty[_], T](param: Param[Ty, T]): String =
    param.annotations.collectFirst {
      case SqlAnnotations.fieldName(str) => str
    }.getOrElse(param.label)

  def findIdField[Ty[_], T](ctx: CaseClass[Ty, T]): Param[Ty, T] =
    ctx.parameters.find { param =>
      val idParam = param.annotations.collectFirst {
        case SqlAnnotations.id() => param
      }
      idParam.isDefined
    }.getOrElse(
      throw new RuntimeException(s"No id field defined for type ${ctx.typeName}. Define one using the @id Annotation")
    )

  def findTableName[Ty[_], T](ctx: CaseClass[Ty, T]): String =
    ctx.annotations.collectFirst {
      case SqlAnnotations.tableName(name) => name
    }.getOrElse(ctx.typeName.short.toLowerCase)

  def findTableName[Ty[_], T](ctx: SealedTrait[Ty, T]): String =
    ctx.annotations.collectFirst {
      case SqlAnnotations.tableName(name) => name
    }.getOrElse(ctx.typeName.short.toLowerCase)

  def findTableName[Ty[_], T](ctx: Subtype[Ty, T]): String =
    ctx.annotations.collectFirst {
      case SqlAnnotations.tableName(name) => name
    }.getOrElse(ctx.typeName.short.toLowerCase)

  def idTypeToString(ft: DataType) = ft match {
    case Float => "FLOAT"
    case Text  => "TEXT"
    case Bool => "BOOLEAN"
    case idType: IdType =>
      idType match {
        case Serial       => "SERIAL"
        case BigSerial    => "BIGSERIAL"
        case Integer      => "INTEGER"
        case Character(n) => s"VARCHAR($n)"
        case UUID         => "UUID"
      }
  }

  def fieldReferenceToFieldType(fieldReference: FieldReference): DataType = fieldReference match {
    case SimpleValue(ft)                      => ft
    case ObjectReference(ft, _, _)            => ft
    case SumTypeObjectReference(baseTable, _) => baseTable.fieldType
  }

  def isAutofillFieldType(fieldType: DataType) = fieldType match {
    case Integer   => false
    case Serial    => true
    case BigSerial => true
    case _         => false
  }

  def fieldRefeenceToTableName(idField: FieldReference): String =
    idField match {
      case ObjectReference(_, tableName, _) => tableName.name
      case SumTypeObjectReference(bt, _)    => bt.tableName.name
      case other =>
        lazy val error =
          s"Expected field $idField to be of type ObjectReference or SumTypeObjectReference, but was $other"
        throw new RuntimeException(error)
    }

  def tableDescriptionForParam[Ty[_], T](param: Param[Ty, T],
                                         fields: List[(ColumnName, TableDescription)],
                                         ctx: CaseClass[Ty, T]): (Param[Ty, T], TableDescription) = {
    val fieldName         = SqlUtils.findFieldName(param)
    lazy val errorMessage = s"Unable to find description for ${param.label} on class ${ctx.typeName.short}"
    val tableDescriptionForParam = fields
      .find(_._1.name == fieldName)
      .getOrElse(throw new RuntimeException(errorMessage))
    (param, tableDescriptionForParam._2)
  }

  def entityDescForParam[Ty[_], T](param: Param[Ty, T],
                                   tableDescRegular: TableDescRegular,
                                   ctx: CaseClass[Ty, T]): (Param[Ty, T], EntityDesc) = {
    val fieldName         = SqlUtils.findFieldName(param)
    val tableName = SqlUtils.findTableName(ctx)
    lazy val errorMessage = s"Unable to find description for ${param.label} on class ${ctx.typeName.short}"
    val entitDescForParam = tableDescRegular.additionalColumns
      .find(_.columnName.name == fieldName)
      .map(_.regularValue)
      .orElse(
        if (tableDescRegular.idColumn.columnName.name == fieldName) Some(IdLeaf(tableDescRegular.idColumn.idValueDesc, TableName(tableName), ColumnName(fieldName)))
        else None
      )
      .getOrElse(throw new RuntimeException(errorMessage))
    (param, entitDescForParam)
  }

  def entityDescForSubtype[Ty[_], T](subtype: Subtype[Ty, T],
                                     tableDesc: TableDescSumType,
                                     ctx: SealedTrait[Ty, T]): EntityDesc = {
    val tableName = SqlUtils.findTableName(subtype)
    lazy val errorMessage =
      s"Unable to find subtype table for type ${subtype.typeName.short} in sealed trait ${ctx.typeName.short}"
    tableDesc.subType
      .find(_.tableName.name == tableName)
      .getOrElse(throw new RuntimeException(errorMessage))
  }

  def narrowToIdDataData(dataType: DataType): IdType = dataType match {
    case Float         => Integer
    case Text          => Character(40)
    case other: IdType => other
  }

  def narrowToAutoIncrementIfPossible(dataType: DataType): IdType = dataType match {
    case Float => Integer
    case Text  => Character(10)
    case idType: IdType =>
      idType match {
        case Serial       => Serial
        case BigSerial    => BigSerial
        case Integer      => Serial
        case Character(n) => Character(n)
        case UUID         => UUID
      }
  }

  def convertToNonAutoIncrementIfPossible(dataType: DataType): DataType = dataType match {
    case Float => Float
    case Text  => Text
    case idType: IdType =>
      idType match {
        case Serial       => Integer
        case BigSerial    => Integer
        case Integer      => Integer
        case Character(n) => Character(n)
        case UUID         => UUID
      }
  }

  def extractResultsStream(fields: List[String], resultSet: ResultSet): IO[List[List[AnyRef]]] = {
    Stream
      .unfoldEval(resultSet) { result =>
        if (result.next()) {
          val pair = (fields.map(f => result.getObject(f)), result)
          IO(Some(pair))
        } else {
          IO(None)
        }

      }
      .compile
      .toList
  }
  def extractFieldsImperative(fields: List[String],
                              resultSet: ResultSet): IO[List[List[AnyRef]]] = {
    IO {
      var omfg = new ListBuffer[List[AnyRef]]
      while (resultSet.next()) {
        val partial = fields.map(name => resultSet.getObject(name))
        omfg += partial
      }
      omfg.toList
    }
  }

  def getJoinList(description: EntityDesc, upstreamTableName: Option[TableName], upstreamColName: Option[ColumnName]): List[JoinDescription] = {
    description match {
      case TableDescRegular(tableName, idColumn, additionalColumns, referencesConstraint, isSubtypeTable, joinOnFind) =>
        val ds = additionalColumns.flatMap(col => getJoinList(col.regularValue, Some(tableName), Some(col.columnName)))
        val fromThis = for {
          ust <- upstreamTableName
          usc <- upstreamColName
        } yield JoinDescription(ust, usc, tableName, idColumn.columnName)

        List(fromThis).flatten ++ ds
      case TableDescSumType(tableName, idColumn, subtypeTableNameCol, subType, joinOnFind) =>
        val ds = subType.flatMap(subType => getJoinList(subType, Some(tableName), Some(subType.idColumn.columnName)))
        val fromThis = for {
          ust <- upstreamTableName
          usc <- upstreamColName
        } yield JoinDescription(ust, usc, tableName, idColumn.columnName)

        List(fromThis).flatten ++ ds
      case TableDescSeqType(tableName, idColumn, entityDesc) =>
        val ds = getJoinList(entityDesc, Some(tableName), Some(idColumn.columnName))
        val fromThis = for {
        ust <- upstreamTableName
        usc <- upstreamColName
        } yield JoinDescription(ust, usc, tableName, idColumn.columnName)
        List(fromThis).flatten ++ ds
      case IdLeaf(idValueDesc, _, _)           =>
        Nil
      case RegularLeaf(dataType, _, _)      =>
        Nil
    }
  }

  def getCompleteColumnList(entityDesc: EntityDesc): List[(TableName, ColumnName)] = {
    entityDesc match {
      case TableDescRegular(tableName, idColumn, additionalColumns, _, _, _) =>
        val colNames = idColumn.columnName :: additionalColumns.toList.map(_.columnName)
        val fromThis = colNames.map(tableName -> _)
        val downStream = additionalColumns.flatMap(col => getCompleteColumnList(col.regularValue))
        fromThis ++ downStream
      case TableDescSumType(tableName, idColumn, subtypeTableNameCol, subType, joinOnFind) =>
        val fromThis = tableName -> idColumn.columnName
        val downStream = subType.toList.flatMap(getCompleteColumnList(_))
        fromThis :: downStream
      case TableDescSeqType(tableName, idColumn, entityDesc) =>
        val fromThis = tableName -> idColumn.columnName
        val downStream = getCompleteColumnList(entityDesc)
        fromThis :: downStream
      case IdLeaf(_, _, _)           =>
        Nil
      case RegularLeaf(_, _, _)      =>
        Nil
    }
  }

  def fullyQualifiedColName(entityDesc: EntityDesc) = {
    entityDesc match {
      case IdLeaf(_, tableName, colName)      => s"${tableName.name}${colName.name}"
      case RegularLeaf(_, tableName, colName) => s"${tableName.name}${colName.name}"
      case other =>
        val errorMessage =
          s"Entity description for was expected to be of type IdLef or RegularLeft, but was $other"
        throw new RuntimeException(errorMessage)
    }
  }
}

object PostgresStuff {
  def go(): (String, Transactor.Aux[IO, Unit]) = {
    val postgres = new EmbeddedPostgres()

    val path = Paths.get("/home/drole/.embedpostgresql")
    val url  = postgres.start(EmbeddedPostgres.cachedRuntimeConfig(path))

    implicit val xa: Transactor.Aux[IO, Unit] = Transactor.fromDriverManager[IO]("org.postgresql.Driver", url)
    (url, xa)
  }

  def goLive(): (String, Transactor.Aux[IO, Unit]) = {
    val url = s"""jdbc:postgresql://0.0.0.0:5432/postgres?user=postgres&password=password"""
    implicit val xa: Transactor.Aux[IO, Unit] = Transactor.fromDriverManager[IO]("org.postgresql.Driver", url)
    (url, xa)
  }
}

sealed class SqlAnnotations extends Annotation
object SqlAnnotations {
  final case class tableName(name: String) extends SqlAnnotations
  final case class fieldName(name: String) extends SqlAnnotations
  final case class id()                    extends SqlAnnotations
  final case class hidden()                extends SqlAnnotations
  final case class ignored()               extends SqlAnnotations
}
