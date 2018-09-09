import SqlAnnotations.{ fieldName, id, tableName }
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

trait Inserter[A] {
  def save(value: A, tableDescription: EntityDesc, assignedId: Option[String] = None)(
    implicit xa: Transactor[IO]
  ): IO[Either[String, String]]
}
object Inserter {

  type Typeclass[T] = Inserter[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): Inserter[T] = new Inserter[T] {
    override def save(value: T, tableDescription: EntityDesc, assignedId: Option[String] = None)(
      implicit xa: doobie.Transactor[IO]
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
  }
  def dispatch[T](ctx: SealedTrait[Typeclass, T]): Inserter[T] = new Inserter[T] {

    override def save(value: T, tableDescription: EntityDesc, assignedId: Option[String] = None)(
      implicit xa: doobie.Transactor[IO]
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
  }

  implicit val intSaver: Inserter[Int] = new Inserter[Int] {
    override def save(value: Int, tableDescription: EntityDesc, assignedId: Option[String] = None)(
      implicit xa: doobie.Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value.toString))
  }

  implicit val doubleSaver: Inserter[Double] = new Inserter[Double] {
    override def save(value: Double, tableDescription: EntityDesc, assignedId: Option[String] = None)(
      implicit xa: doobie.Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value.toString))
  }

  implicit val stringSaver: Inserter[String] = new Inserter[String] {
    override def save(value: String, tableDescription: EntityDesc, assignedId: Option[String] = None)(
      implicit xa: doobie.Transactor[IO]
    ): IO[Either[String, String]] =
      IO(Right(value))
  }

  implicit def seqSaver[A](implicit inserter: Inserter[A]): Inserter[List[A]] = new Typeclass[List[A]] {
    override def save(value: List[A], tableDescription: EntityDesc, assignedId: Option[String])(
      implicit xa: doobie.Transactor[IO]
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
        upstreamResults <- value.traverse(v => inserter.save(v, desc.entityDesc, None))
        omg             = upstreamResults.map(_.right.get)
        progs = omg.map { res =>
          val sql = s"""insert into $tableName ($upstreamColName, $downstreamColName) values ('$id', '$res')"""
          println(sql)
          Fragment.const(sql)
        }
        meh <- progs.traverse(prog => prog.updateWithLogHandler(LogHandler.jdkLogHandler).run.transact(xa))
      } yield Right(meh.head.toString)

    }
  }

  implicit def gen[T]: Inserter[T] = macro Magnolia.gen[T]
}

object TestSave extends App {
  implicit val (url, xa) = PostgresStuff.go()

//  case class A(@id a: String, b: Int, c: Double)
//  case class B(a: A, @id d: String)
//  case class C(@id e: Int, b: B)
//
//  val describer   = TableDescriber.gen[C]
//  val description = describer.describe(false, false)
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
//  val prog = for {
//    result       <- creator.createTable(description)
//    beforeInsert <- listItemsInCProg.transact(xa)
//    idRes        <- saver.save(testValue, description)
//    afterInsert  <- listItemsInCProg.transact(xa)
//  } yield (beforeInsert, idRes, afterInsert)
//
//  val (before, res, after) = prog.unsafeRunSync()
//  println(before.size)
//  println(after.size)
//  println(after.diff(before))

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
  val testValueTwo = Accountant(3, "Sally", 30.0, List(Book(1, "A Nice Book", 2), Book(3,"A better book", 2001)))

  val prog2 = for {
    result     <- creator2.createTable(description2)
    empsBefore <- findEmployeesProg.transact(xa)
    booksBefore <- findBooksProg.transact(xa)
    _          <- inserter.save(testValueOne, description2)
    _          <- inserter.save(testValueTwo, description2)
    empsAfter  <- findEmployeesProg.transact(xa)
  booksAfter <- findBooksProg.transact(xa)
  } yield (result, empsBefore, empsAfter, booksBefore, booksAfter)

  val (res2, before3, after3, bb, ba) = prog2.unsafeRunSync()
  println(after3)
  println(after3.diff(before3))
  println(bb)
  println(ba)
}
