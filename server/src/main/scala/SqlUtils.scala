import java.nio.file.Paths

import LetsDoThis.{ Default, TypeNameInfo }
import SqlAnnotations._
import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.implicits._
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres._
import magnolia.{ Param, _ }
import shapeless.tupled._
import scala.concurrent.duration._

import scala.annotation.Annotation
import scala.language.experimental.macros
import java.nio.file.Paths

import cats.effect.IO
import doobie.{ LogHandler, Transactor }
import magnolia.{ CaseClass, Param, SealedTrait, Subtype }
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
object SqlUtils {
  def idTypeString[A](a: A): Option[String] =
    a match {
      case Int => Some("SERIAL")
      case _ => None
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
      throw new RuntimeException(s"No id field defined for type ${ctx.typeName}. Define one using the @id Annotation"))

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
    case Text => "TEXT"
    case idType: IdType =>
      idType match {
        case Serial => "SERIAL"
        case BigSerial => "BIGSERIAL"
        case Integer => "INTEGER"
        case Character(n) => s"VARCHAR($n)"
        case UUID => "UUID"
      }
  }

  def fieldReferenceToFieldType(fieldReference: FieldReference): DataType = fieldReference match {
    case SimpleValue(ft) => ft
    case ObjectReference(ft, _, _) => ft
    case SumTypeObjectReference(baseTable, _) => baseTable.fieldType
  }

  def isAutofillFieldType(fieldType: DataType) = fieldType match {
    case Integer => false
    case Serial => true
    case BigSerial => true
    case _ => false
  }

  def fieldRefeenceToTableName(idField: FieldReference): String =
    idField match {
      case ObjectReference(_, tableName, _) => tableName.name
      case SumTypeObjectReference(bt, _) => bt.tableName.name
      case other =>
        lazy val error =
          s"Expected field $idField to be of type ObjectReference or SumTypeObjectReference, but was $other"
        throw new RuntimeException(error)
    }

  def tableDescriptionForParam[Ty[_], T](
    param: Param[Ty, T],
    fields: List[(ColumnName, TableDescription)],
    ctx: CaseClass[Ty, T]): (Param[Ty, T], TableDescription) = {
    val fieldName = SqlUtils.findFieldName(param)
    lazy val errorMessage = s"Unable to find description for ${param.label} on class ${ctx.typeName.short}"
    val tableDescriptionForParam = fields
      .find(_._1.name == fieldName)
      .getOrElse(throw new RuntimeException(errorMessage))
    (param, tableDescriptionForParam._2)
  }

  def entityDescForParam[Ty[_], T](
    param: Param[Ty, T],
    tableDescRegular: TableDescRegular,
    ctx: CaseClass[Ty, T]): (Param[Ty, T], EntityDesc) = {
    val fieldName = SqlUtils.findFieldName(param)
    lazy val errorMessage = s"Unable to find description for ${param.label} on class ${ctx.typeName.short}"
    val entitDescForParam = tableDescRegular.additionalColumns
      .find(_.columnName.name == fieldName)
      .map(_.regularValue)
      .orElse(
        if (tableDescRegular.idColumn.columnName.name == fieldName) Some(IdLeaf(tableDescRegular.idColumn.idValueDesc))
        else None)
      .getOrElse(throw new RuntimeException(errorMessage))
    (param, entitDescForParam)
  }

  def entityDescForSubtype[Ty[_], T](
    subtype: Subtype[Ty, T],
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
    case Float => Integer
    case Text => Character(40)
    case other: IdType => other
  }

  def narrowToAutoIncrementIfPossible(dataType: DataType): IdType = dataType match {
    case Float => Integer
    case Text => Character(10)
    case idType: IdType =>
      idType match {
        case Serial => Serial
        case BigSerial => BigSerial
        case Integer => Serial
        case Character(n) => Character(n)
        case UUID => UUID
      }
  }

  def convertToNonAutoIncrementIfPossible(dataType: DataType): DataType = dataType match {
    case Float => Float
    case Text => Text
    case idType: IdType =>
      idType match {
        case Serial => Integer
        case BigSerial => Integer
        case Integer => Integer
        case Character(n) => Character(n)
        case UUID => UUID
      }
  }
}

object PostgresStuff {
  def go(): (String, Transactor.Aux[IO, Unit]) = {
    val postgres = new EmbeddedPostgres()

    val path = Paths.get("/home/drole/.embedpostgresql")
    val url = postgres.start(EmbeddedPostgres.cachedRuntimeConfig(path))

    implicit val xa: Transactor.Aux[IO, Unit] = Transactor.fromDriverManager[IO]("org.postgresql.Driver", url)
    (url, xa)
  }
}

sealed class SqlAnnotations extends Annotation
object SqlAnnotations {
  final case class tableName(name: String) extends SqlAnnotations
  final case class fieldName(name: String) extends SqlAnnotations
  final case class id() extends SqlAnnotations
  final case class hidden() extends SqlAnnotations
  final case class ignored() extends SqlAnnotations
}