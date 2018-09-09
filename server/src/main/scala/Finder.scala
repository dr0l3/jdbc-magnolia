import cats.Traverse
import cats.effect.IO
import doobie.{ Fragment, LogHandler, Transactor }
import magnolia.{ CaseClass, Magnolia, SealedTrait }
import SqlAnnotations.{ fieldName, id, tableName }
import TestSave.xa
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
import org.scalacheck.Arbitrary
import scalaprops.Gen
import scalaprops.Shapeless._

import scala.util.Try
trait Finder[A] {
  def findById(id: String, tableDescription: EntityDesc)(implicit xa: Transactor[IO]): IO[Option[A]]
}

object Finder {
  type Typeclass[T] = Finder[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): Finder[T] = new Finder[T] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: doobie.Transactor[IO]): IO[Option[T]] = {
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
          case _ => false
        }

        if(isSeqType){
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


          val res =queryResult.flatMap { maybeString =>
            maybeString
              .map(
                str =>
                  param.typeclass.findById(
                    str,
                    descForParam
                  )
              )
              .getOrElse(IO(None))
          }
          res
        }

      }

      for {
        subResults <- subPrograms
      } yield Try(Some(ctx.rawConstruct(subResults.map(_.get)))).getOrElse(None)
    }

  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): Finder[T] = new Finder[T] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: doobie.Transactor[IO]): IO[Option[T]] = {
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
  }

  implicit val intFinder = new Finder[Int] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: doobie.Transactor[IO]): IO[Option[Int]] =
      IO(Some(id.toInt))
  }

  implicit val stringFinder = new Finder[String] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: doobie.Transactor[IO]): IO[Option[String]] =
      IO(Some(id))
  }

  implicit val doubleFinder = new Finder[Double] {
    override def findById(id: String, tableDescription: EntityDesc)(implicit
                                                                    xa: doobie.Transactor[IO]): IO[Option[Double]] =
      IO(Some(id.toDouble))
  }

  implicit def seqFinder[A](implicit finder: Finder[A]) = new Finder[List[A]] {
    override def findById(id: String, tableDescription: EntityDesc)(
      implicit xa: Transactor[IO]
    ): IO[Option[List[A]]] = {
      println(s"LIST FINDER")
      val desc = tableDescription match {
        case t: TableDescSeqType => t
        case other =>
          val errorMessage = s"Entity Desc for list type was expected to be of type TableDescSeqType, but was $other"
          throw new RuntimeException(errorMessage)
      }

      val tableName = desc.tableName.name
      val idColName = desc.idColumn.columnName.name
      println(tableDescription)
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
        res <- ids.traverse(id => finder.findById(id, desc.entityDesc))
      } yield res.sequence
    }
  }

  implicit def gen[T]: Finder[T] = macro Magnolia.gen[T]
}

object TestFind extends App {
  implicit val (url, xa) = PostgresStuff.go()

//  case class A(@id a: String, b: Int, c: Double)
//  case class B(a: A, @id d: String)
//  case class C(@id e: Int, b: B)
//
//  val describer   = TableDescriber.gen[C]
//  val description = describer.describe(false, false, TableName(""), null)
//
//  println(description)
//
//  val listTablesProg = Fragment
//    .const(s"SELECT table_name FROM information_schema.tables ORDER BY table_name;")
//    .query[String]
//    .to[List]
//
//  val listItemsInCProg = Fragment
//    .const("SELECT * FROM c")
//    .query[(Int, String)]
//    .to[List]
//
//  val creator   = TableCreator.gen[C]
//  val saver     = Inserter.gen[C]
//  val testValue = C(0, B(A("test-id-1", 2, 3.0), "test-id-2"))
//  println(testValue)
//  val finder1 = Finder.gen[C]
//  val prog = for {
//    result       <- creator.createTable(description)
//    beforeInsert <- listItemsInCProg.transact(xa)
//    idRes        <- saver.save(testValue, description)
//    afterInsert  <- listItemsInCProg.transact(xa)
//    res          <- finder1.findById("1", description)
//  } yield (beforeInsert, idRes, afterInsert, res)
//
//  val (before, res, after, found) = prog.unsafeRunSync()
//  println(before.size)
//  println(after.size)
//  println(after.diff(before))
//  println(found)

  case class Book(@id @fieldName("book_id") bookId: Int, name: String, published: Int)

  @tableName("employees") sealed trait Employee
  @tableName("janitors") case class Janitor(@id id: Int, name: String, age: Int) extends Employee
  @tableName("accountants") case class Accountant(@id id: Int, name: String, salary: Double, books: List[Book])
      extends Employee

  val describer2   = TableDescriber.gen[Employee]
  val description2 = describer2.describe(false, false, TableName(""), null)

  val findEmployeesProg = Fragment
    .const(s"select * from employees")
    .queryWithLogHandler[(Int)](LogHandler.jdkLogHandler)
    .to[List]

  val findBooksProg = Fragment
    .const("select * from book")
    .queryWithLogHandler[Book](LogHandler.jdkLogHandler)
    .to[List]

  val creator2 = TableCreator.gen[Employee]
  val inserter = Inserter.gen[Employee]

  val testValueOne = Janitor(1, "Dr. Jan Itor", 42)
  val testValueTwo = Accountant(3, "Sally", 30.0, List(Book(1, "A Nice Book", 2), Book(3, "A better book", 2001)))

  val finder = Finder.gen[Employee]

  val prog2 = for {
    result      <- creator2.createTable(description2)
    empsBefore  <- findEmployeesProg.transact(xa)
    booksBefore <- findBooksProg.transact(xa)
    id1         <- inserter.save(testValueOne, description2)
    id2         <- inserter.save(testValueTwo, description2)
    empsAfter   <- finder.findById(id1.right.get, description2)
    booksAfter  <- finder.findById(id2.right.get, description2)
  } yield (result, empsBefore, empsAfter, booksBefore, booksAfter)

  val (res2, before3, after3, bb, ba) = prog2.unsafeRunSync()
  println(after3)
  println(bb)
  println(ba)
}
