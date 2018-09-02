import cats.Traverse
import cats.effect.IO
import doobie.{ Fragment, LogHandler, Transactor }
import magnolia.{ CaseClass, Magnolia, SealedTrait }
import SqlAnnotations.{ id, tableName }
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
        val fieldName = SqlUtils.findFieldName(param)
        val sqlStr    = s"select $fieldName from $tableName where $idFieldName = '$id'"
        val queryResult = Fragment
          .const(sqlStr)
          .queryWithLogHandler[String](LogHandler.jdkLogHandler)
          .to[List]
          .transact(xa)
          .map(_.headOption)

        val (_, descForParam) = SqlUtils.entityDescForParam(param, tableDesc, ctx)
        queryResult.flatMap { maybeString =>
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

  implicit def gen[T]: Finder[T] = macro Magnolia.gen[T]

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
}

object TestFind extends App {
  implicit val (url, xa) = PostgresStuff.go()

  case class A(@id a: String, b: Int, c: Double)
  case class B(a: A, @id d: String)
  case class C(@id e: Int, b: B)

  val describer   = TableDescriber.gen[C]
  val description = describer.describe(false, false)

  println(description)

  val listTablesProg = Fragment
    .const(s"SELECT table_name FROM information_schema.tables ORDER BY table_name;")
    .query[String]
    .to[List]

  val listItemsInCProg = Fragment
    .const("SELECT * FROM c")
    .query[(Int, String)]
    .to[List]

  val creator   = TableCreator.gen[C]
  val saver     = Inserter.gen[C]
  val testValue = C(0, B(A("test-id-1", 2, 3.0), "test-id-2"))
  println(testValue)
  val finder1 = Finder.gen[C]
  val prog = for {
    result       <- creator.createTable(description)
    beforeInsert <- listItemsInCProg.transact(xa)
    idRes        <- saver.save(testValue, description)
    afterInsert  <- listItemsInCProg.transact(xa)
    res          <- finder1.findById("1", description)
  } yield (beforeInsert, idRes, afterInsert, res)

  val (before, res, after, found) = prog.unsafeRunSync()
  println(before.size)
  println(after.size)
  println(after.diff(before))
  println(found)

  @tableName("employees") sealed trait Employee
  @tableName("janitors") case class Janitor(@id id: Int, name: String, age: Int)             extends Employee
  @tableName("accountants") case class Accountant(@id id: Int, name: String, salary: Double) extends Employee

  val describer2   = TableDescriber.gen[Employee]
  val description2 = describer2.describe(false, false)

  val findEmployeesProg = Fragment
    .const(s"select * from employees")
    .queryWithLogHandler[(Int)](LogHandler.jdkLogHandler)
    .to[List]

  val creator2 = TableCreator.gen[Employee]
  val inserter = Inserter.gen[Employee]

  val testValueOne = Janitor(1, "Dr. Jan Itor", 42)
  val testValueTwo = Accountant(3, "Sally", 30.0)
  val finder2      = Finder.gen[Employee]

  val prog2 = for {
    result     <- creator2.createTable(description2)
    empsBefore <- findEmployeesProg.transact(xa)
    _          <- inserter.save(testValueOne, description2)
    _          <- inserter.save(testValueTwo, description2)
    empsAfter  <- findEmployeesProg.transact(xa)
    res        <- finder2.findById("1", description2)
    res2       <- finder2.findById("2", description2)
    res3       <- finder2.findById("3", description2)
  } yield (result, empsBefore, empsAfter, res, res2, res3)

  val (res2, before3, after3, foundEmp, foundEmp2, foundEmp3) = prog2.unsafeRunSync()
  println(after3)
  println(after3.diff(before3))
  println(foundEmp)
  println(foundEmp2)
  println(foundEmp3)
}
