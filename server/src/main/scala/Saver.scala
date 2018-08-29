import java.nio.file.Paths

import EasyUpdater.Typeclass
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

@tableName("cabins")
case class Cabin(@id id: Int,
                 @fieldName("owner_id") owner: Person,
                 @fieldName("square_meters") squareMeters: Int,
                 temperature: Double)
case class Person(@id @fieldName("person_id") personId: Int,
                  @fieldName("person_name") name: String,
                  age: Int,
                  @fieldName("pet_id") pet: Pet)
@tableName("pets")
case class Pet(@fieldName("pet_id") @id petId: Int, @fieldName("pet_name") name: String)

object PostgresStuff {
  def go(): (String, Transactor.Aux[IO, Unit]) = {
    val postgres = new EmbeddedPostgres()

    val path = Paths.get("/home/drole/.embedpostgresql")
    val url  = postgres.start(EmbeddedPostgres.cachedRuntimeConfig(path))

    implicit val xa: Transactor.Aux[IO, Unit] = Transactor.fromDriverManager[IO]("org.postgresql.Driver", url)
    (url, xa)
  }

  def create3()(implicit xa: Transactor[IO]): IO[Int] =
    sql""" create table cabins (
           id SERIAL,
           square_meters integer,
           owner_id integer,
           temperature float,
           primary key (id),
           foreign key (owner_id) references person(person_id)
    );
       """
      .updateWithLogHandler(LogHandler.jdkLogHandler)
      .run
      .transact(xa)

  def create2()(implicit xa: Transactor[IO]): IO[Int] =
    sql""" CREATE TABLE person (
           person_id SERIAL,
           person_name TEXT NOT NULL,
           age SMALLINT NOT NULL,
           pet_id integer,
           primary key (person_id),
           foreign key (pet_id) references pets(pet_id)
     );
       """
      .updateWithLogHandler(LogHandler.jdkLogHandler)
      .run
      .transact(xa)

  def create()(implicit xa: Transactor[IO]): IO[Int] =
    sql""" CREATE TABLE pets (
           pet_id SERIAL,
           pet_name TEXT NOT NULL,
           primary key (pet_id)
     );
       """
      .updateWithLogHandler(LogHandler.jdkLogHandler)
      .run
      .transact(xa)

  def insert(person: Person)(implicit xa: Transactor[IO]): IO[Int] = {
    val age     = person.age
    val name    = person.name
    val petName = person.pet.name

    val prog = for {
      petId <- sql"""insert into pets (pet_name) values ($petName)"""
                .updateWithLogHandler(LogHandler.jdkLogHandler)
                .withUniqueGeneratedKeys[Int]("pet_id")
      pId <- sql"""insert into person (age, pet_id, person_name) values ( $age , $petId , $name )"""
              .updateWithLogHandler(LogHandler.jdkLogHandler)
              .withUniqueGeneratedKeys[Int]("person_id")
    } yield pId

    prog.transact(xa)
  }

  def find2(id: Int)(implicit xa: Transactor[IO]): IO[List[Person]] =
    for {
      response <- sql"""select person.person_id, person.person_name, person.age, pets.pet_id, pets.pet_name from person full outer join pets on person.pet_id = pets.pet_id where person.person_id = $id"""
                   .queryWithLogHandler[Person](LogHandler.jdkLogHandler)
                   .to[List]
                   .transact(xa)
    } yield response

  def find(id: Int)(implicit xa: Transactor[IO]) =
    for {
      petIds <- sql"""select pet_id from person where person_id = $id """
                 .queryWithLogHandler[Int](LogHandler.jdkLogHandler)
                 .to[List]
                 .transact(xa)
      petId = petIds.head
      petNames <- sql"""select pet_name from pets where pet_id = $petId """
                   .queryWithLogHandler[String](LogHandler.jdkLogHandler)
                   .to[List]
                   .transact(xa)
      petName = petNames.head
      personVitals <- sql"""select person_name, age from person where person_id = $id """
                       .queryWithLogHandler[(String, Int)](LogHandler.jdkLogHandler)
                       .to[List]
                       .transact(xa)
    } yield {
      val personVital = personVitals.head
      Person(id, personVital._1, personVital._2, Pet(petId, petName))
    }
  def getAllPets()(implicit xa: Transactor[IO]): IO[List[Pet]] =
    sql"""select * from pets"""
      .query[Pet]
      .to[List]
      .transact(xa)

}

sealed trait ObjectReplication
case object Inline     extends ObjectReplication
case class OneToOne()  extends ObjectReplication
case class OneToMany() extends ObjectReplication

sealed trait SequenceReplication
case object InlineArray extends SequenceReplication
case class ManyToTo()   extends SequenceReplication
case class ManyToMany() extends SequenceReplication

sealed class SqlAnnotations extends Annotation
object SqlAnnotations {
  final case class tableName(name: String)                          extends SqlAnnotations
  final case class fieldName(name: String)                          extends SqlAnnotations
  final case class id()                                             extends SqlAnnotations
  final case class hidden()                                         extends SqlAnnotations
  final case class ignored()                                        extends SqlAnnotations
  final case class replicationObj(replication: ObjectReplication)   extends SqlAnnotations
  final case class replicationSeq(replication: SequenceReplication) extends SqlAnnotations
}

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
      throw new RuntimeException(s"No id field defined for type ${ctx.typeName}. Define one using the @id Annotation"))

  def findTableName[Ty[_], T](ctx: CaseClass[Ty, T]): String =
    ctx.annotations.collectFirst {
      case SqlAnnotations.tableName(name) => name
    }.getOrElse(ctx.typeName.short)

  def idTypeToString(ft: FieldType) = ft match {
    case UUID => "TEXT"
    case Integer => "SERIAL"
    case Double => "FLOAT"
  }
}

sealed trait FieldType
case object UUID extends FieldType
case object Integer extends FieldType
case object Double extends FieldType

case class TableName(name: String) extends AnyVal
case class FieldName(name: String) extends AnyVal

trait FieldReference
case class SimpleValue(fieldType: FieldType) extends FieldReference
case class ObjectReference(fieldType: FieldType, tableName: TableName, fieldName: FieldName) extends FieldReference

case class TableDescription(idField: FieldReference, additionalFields: List[(FieldName, FieldReference)])

trait TableDescriber[A] {
  def describe(): TableDescription
}

object TableDescriber {
  type Typeclass[T] = TableDescriber[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): TableDescriber[T] = () => {
    val tableName   = SqlUtils.findTableName(ctx)
    val idField     = SqlUtils.findIdField(ctx)
    val idFieldName = SqlUtils.findFieldName(idField)
    val remaining   = ctx.parameters.filterNot(_ == idField)
    val nonIdFieldStructure = remaining.map { param =>
      val fieldName      = SqlUtils.findFieldName(param)
      val fieldReference = param.typeclass.describe().idField
      FieldName(fieldName) -> fieldReference
    }.toList

    val idFieldType = idField.typeclass.describe().idField match {
      case SimpleValue(ft)           => ft
      case ObjectReference(ft, _, _) => ft
    }

    val idFieldStructure = ObjectReference(idFieldType, TableName(tableName), FieldName(idFieldName))
    TableDescription(idFieldStructure, nonIdFieldStructure)
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): TableDescriber[T] = ???

  implicit def gen[T]: TableDescriber[T] = macro Magnolia.gen[T]

  implicit val intUpdater: TableDescriber[Int] = () => TableDescription(SimpleValue(Integer), Nil)

  implicit val stringUpdater: TableDescriber[String] = () => TableDescription(SimpleValue(UUID), Nil)

  implicit val doubleUpdater: TableDescriber[Double] = () => TableDescription(SimpleValue(Double), Nil)
}

trait EasyUpdater[A] {
  def update(value: A)(implicit xa: Transactor[IO]): Either[String, IO[String]]
}

object EasyUpdater {

  type Typeclass[T] = EasyUpdater[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): EasyUpdater[T] = new EasyUpdater[T] {
    override def update(value: T)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = {
      // Find the tablename
      val tableName = ctx.annotations.collectFirst {
        case SqlAnnotations.tableName(name) => name
      }.getOrElse(ctx.typeName.short)

      // Find the id field
      val idField = ctx.parameters.find { param =>
        val idParam = param.annotations.collectFirst {
          case SqlAnnotations.id() => param
        }
        idParam.isDefined
      }.getOrElse(
        throw new RuntimeException(s"No id field defined for type ${ctx.typeName}. Define one using the @id Annotation")
      )

      val remainingParams = ctx.parameters.filterNot(_ == idField)

      val labels = ctx.parameters.map { param =>
        val annotationLabel = param.annotations.collectFirst {
          case SqlAnnotations.fieldName(name) => name
        }
        annotationLabel.getOrElse(param.label)
      }
      val values = ctx.parameters.map(param => param.typeclass.update(param.dereference(value)))

      val updates = values
        .foldLeft[List[String]](Nil)((acc, next) => {
        next match {
          case Right(v) => v.unsafeRunSync() :: acc
          case Left(v)  => v :: acc
        }
      })
        .reverse
      val fieldsString = labels.mkString("(", ", ", ")")
      val valuesString = updates.map(str => s"'$str'").mkString("(", ", ", ")")
      val onConflictString = s" on conflict (${SqlUtils.findFieldName(SqlUtils.findIdField(ctx))})  do "
      val fieldUpdates = remainingParams.map(SqlUtils.findFieldName).zip(updates.tail).map { case (fieldName, fieldValue) => s"$fieldName = '$fieldValue'"}.mkString(", ")
      val updateString = s"update set $fieldUpdates"
      val update = Fragment.const("""insert into """) ++
        Fragment.const(tableName) ++
        Fragment.const(fieldsString) ++
        Fragment.const(" values ") ++
        Fragment.const(valuesString) ++
        Fragment.const(onConflictString) ++
        Fragment.const(updateString)
      val idCol = ctx.parameters.map { param =>
        val annotationId = param.annotations.collectFirst {
          case SqlAnnotations.id() => {
            val annotationFieldname = param.annotations.collectFirst {
              case SqlAnnotations.fieldName(name) => name
            }
            annotationFieldname.getOrElse(param.label)
          }
        }
        annotationId.getOrElse(s"id")
      }
      println(update.toString())
      Right(
        update.updateWithLogHandler(LogHandler.jdkLogHandler).withUniqueGeneratedKeys[String](idCol.head).transact(xa)
      )
    }
  }
  def dispatch[T](ctx: SealedTrait[Typeclass, T]): EasyUpdater[T] = new EasyUpdater[T] {

    override def update(value: T)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] =
      ctx.dispatch(value) { sub =>
        sub.typeclass.update(sub.cast(value))
      }
  }

  implicit val intUpdater: EasyUpdater[Int] = new EasyUpdater[Int] {
    override def update(value: Int)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = Left(value.toString)
  }

  implicit val doubkeUpdater: EasyUpdater[Double] = new EasyUpdater[Double] {
    override def update(value: Double)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] =
      Left(value.toString)
  }

  implicit val stringUpdater: EasyUpdater[String] = new EasyUpdater[String] {
    override def update(value: String)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = Left(value)
  }

  implicit def gen[T]: EasyUpdater[T] = macro Magnolia.gen[T]
}

trait EasyFinder[A] {
  def findById(id: String)(implicit xa: Transactor[IO]): IO[Option[A]]
}

object EasyFinder {
  type Typeclass[T] = EasyFinder[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): EasyFinder[T] = new EasyFinder[T] {
    override def findById(id: String)(implicit
                                      xa: doobie.Transactor[IO]): IO[Option[T]] = {
      val tableName   = SqlUtils.findTableName(ctx)
      val idField     = SqlUtils.findIdField(ctx)
      val idFieldName = SqlUtils.findFieldName(idField)

      val subPrograms: List[IO[Option[Any]]] = ctx.parameters.toList.map { param =>
        val fieldName = SqlUtils.findFieldName(param)
        val sqlStr    = s"select $fieldName from $tableName where $idFieldName = $id"
        val res = Fragment
          .const(sqlStr)
          .queryWithLogHandler[String](LogHandler.jdkLogHandler)
          .to[List]
          .transact(xa)
          .map(_.headOption)
        res.flatMap { str =>
          param.typeclass.findById(
            str.getOrElse(throw new RuntimeException("Trying to look up a value that does not exists"))
          )
        }
      }

      val s: IO[Option[T]] = Traverse[List].sequence(subPrograms).map(omg => Some(ctx.rawConstruct(omg.map(_.get))))
      s
    }

  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): EasyFinder[T] = new EasyFinder[T] {
    override def findById(id: String)(implicit
                                      xa: doobie.Transactor[IO]): IO[Option[T]] = ???
  }

  implicit def gen[T]: EasyFinder[T] = macro Magnolia.gen[T]

  implicit val intFinder = new EasyFinder[Int] {
    override def findById(id: String)(implicit
                                      xa: doobie.Transactor[IO]): IO[Option[Int]] = IO(Some(id.toInt))
  }

  implicit val stringFinder = new EasyFinder[String] {
    override def findById(id: String)(implicit
                                      xa: doobie.Transactor[IO]): IO[Option[String]] = IO(Some(id))
  }

  implicit val doubleFinder = new EasyFinder[Double] {
    override def findById(id: String)(implicit
                                      xa: doobie.Transactor[IO]): IO[Option[Double]] = IO(Some(id.toDouble))
  }
}

trait EasyCreator[A] {
  def createTable()(implicit xa: Transactor[IO]): Either[String, (String, String, IO[Int])]
}

object EasyCreator {
  type Typeclass[T] = EasyCreator[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): EasyCreator[T] = new EasyCreator[T] {
    override def createTable()(implicit xa: doobie.Transactor[IO]): Either[String, (String, String, IO[Int])] = {
      // Find the tablename
      val tableName = SqlUtils.findTableName(ctx)

      // Find the id field
      val idField           = SqlUtils.findIdField(ctx)
      val crudeType         = idField.typeclass.createTable().left.get
      val refinedType       = if (crudeType.contains("INTEGER")) "SERIAL" else crudeType
      val idFieldDefinition = s"${SqlUtils.findFieldName(idField)} $refinedType"

      val remainingParams = ctx.parameters.filterNot(_ == idField)

      // Partition remaining param into primitive and references
      val (primitive, reference) = remainingParams.partition { param =>
        param.typeclass.createTable().isLeft
      }

      // Create field definition for each field
      // If primitive
      val primitiveFieldDefintions = primitive.map { param =>
        val fieldType = param.typeclass.createTable().left.get
        s"${SqlUtils.findFieldName(param)} ${fieldType}"
      }

      // If reference
      val referenceFieldDefinitions = reference.map { param =>
        val (fieldType, _, _) = param.typeclass.createTable().right.get
        s"${SqlUtils.findFieldName(param)} ${fieldType}"
      }

      // Foreign keys
      val foreignKeyDefintions = reference.map { param =>
        val (_, referenceIdCol, _) = param.typeclass.createTable().right.get
        s"foreign key (${SqlUtils.findFieldName(param)}) references $referenceIdCol"
      }

      // Primary key
      val primaryKey = s"primary key (${SqlUtils.findFieldName(idField)})"
      val definitions = Seq(idFieldDefinition) ++ primitiveFieldDefintions ++ referenceFieldDefinitions ++ Seq(
        primaryKey
      ) ++ foreignKeyDefintions
      val tableDefinition = s"create table if not exists ${tableName} (${definitions.mkString(", ")})"
      val referencePrograms =
        reference.map(param => param.typeclass.createTable().right.get._3).toList
      val res = Fragment.const(tableDefinition).updateWithLogHandler(LogHandler.jdkLogHandler).run.transact(xa)
      val all = for {
        _   <- referencePrograms.sequence
        omg <- res
      } yield omg

      Right(
        (idField.typeclass.createTable().left.get,
         s"$tableName(${SqlUtils.findFieldName(idField)})",
         all)
      )
    }
  }

  def dispatch[T](ctx: SealedTrait[Typeclass, T]): EasyCreator[T] = new EasyCreator[T] {
    override def createTable()(implicit xa: doobie.Transactor[IO]): Either[String, (String, String, IO[Int])] = ???
  }

  implicit val intCreator = new EasyCreator[Int] {
    override def createTable()(implicit xa: doobie.Transactor[IO]): Either[String, (String, String, IO[Int])] =
      Left("INTEGER")
  }

  implicit val strCreator = new EasyCreator[String] {
    override def createTable()(implicit xa: doobie.Transactor[IO]): Either[String, (String, String, IO[Int])] =
      Left("TEXT")
  }

  implicit val doubleCreator = new EasyCreator[Double] {
    override def createTable()(implicit xa: doobie.Transactor[IO]): Either[String, (String, String, IO[Int])] =
      Left("FLOAT")
  }

  implicit def gen[T]: EasyCreator[T] = macro Magnolia.gen[T]
}

trait EasySaver[A] {
  def save(value: A)(implicit xa: Transactor[IO]): Either[String, IO[String]]
}
object EasySaver {

  type Typeclass[T] = EasySaver[T]

  def combine[T](ctx: CaseClass[Typeclass, T]): EasySaver[T] = new EasySaver[T] {
    override def save(value: T)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = {
      // Find the tablename
      val tableName = ctx.annotations.collectFirst {
        case SqlAnnotations.tableName(name) => name
      }.getOrElse(ctx.typeName.short)

      // Find the id field
      val idField = ctx.parameters.find { param =>
        val idParam = param.annotations.collectFirst {
          case SqlAnnotations.id() => param
        }
        idParam.isDefined
      }.getOrElse(
        throw new RuntimeException(s"No id field defined for type ${ctx.typeName}. Define one using the @id Annotation")
      )

      val remainingParams = ctx.parameters.filterNot(_ == idField)

      val labels = remainingParams.map { param =>
        val annotationLabel = param.annotations.collectFirst {
          case SqlAnnotations.fieldName(name) => name
        }
        annotationLabel.getOrElse(param.label)
      }
      val values = remainingParams.map(param => param.typeclass.save(param.dereference(value)))

      val updates = values
        .foldLeft[List[String]](Nil)((acc, next) => {
          next match {
            case Right(v) => v.unsafeRunSync() :: acc
            case Left(v)  => v :: acc
          }
        })
        .reverse
      val fieldsString = labels.mkString("(", ", ", ")")
      val valuesString = updates.map(str => s"'$str'").mkString("(", ", ", ")")
      val update = Fragment.const("""insert into """) ++
        Fragment.const(tableName) ++
        Fragment.const(fieldsString) ++
        Fragment.const(" values ") ++
        Fragment.const(valuesString)
      val idCol = ctx.parameters.map { param =>
        val annotationId = param.annotations.collectFirst {
          case SqlAnnotations.id() => {
            val annotationFieldname = param.annotations.collectFirst {
              case SqlAnnotations.fieldName(name) => name
            }
            annotationFieldname.getOrElse(param.label)
          }
        }
        annotationId.getOrElse(s"id")
      }
      println(update.toString())
      Right(
        update.updateWithLogHandler(LogHandler.jdkLogHandler).withUniqueGeneratedKeys[String](idCol.head).transact(xa)
      )
    }
  }
  def dispatch[T](ctx: SealedTrait[Typeclass, T]): EasySaver[T] = new EasySaver[T] {

    override def save(value: T)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] =
      ctx.dispatch(value) { sub =>
        sub.typeclass.save(sub.cast(value))
      }
  }

  implicit val intSaver: EasySaver[Int] = new EasySaver[Int] {
    override def save(value: Int)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = Left(value.toString)
  }

  implicit val doubkeSaver: EasySaver[Double] = new EasySaver[Double] {
    override def save(value: Double)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] =
      Left(value.toString)
  }

  implicit val stringSaver: EasySaver[String] = new EasySaver[String] {
    override def save(value: String)(implicit xa: doobie.Transactor[IO]): Either[String, IO[String]] = Left(value)
  }

  implicit def gen[T]: EasySaver[T] = macro Magnolia.gen[T]
}

object Desc extends App {
  val desc = TableDescriber.gen[Cabin]

  println(desc.describe())
}

object Saver extends App {
  implicit val (url, xa) = PostgresStuff.go()

  val testPerson  = Person(0, "Rune", 32, Pet(1, "snuffles"))
  val testPerson2 = Person(2, "Sofie", 27, Pet(2, "piggy"))

  val testCabin = Cabin(0, Person(0, "The boss", 2, Pet(0, "Ruffless")), 20, 20.0)

  case class Test2(@id id: String, n: Int)
  case class Test3(@id id: Int, test2: Test2)

  val saver    = EasySaver.gen[Person]
  val saver2   = EasySaver.gen[Cabin]
  val creator  = EasyCreator.gen[Test2]
  val creator2 = EasyCreator.gen[Test3]
  val creator3 = EasyCreator.gen[Pet]
  val creator4 = EasyCreator.gen[Person]
  val creator5 = EasyCreator.gen[Cabin]
  val updater = EasyUpdater.gen[Cabin]

  val t  = Test2("omg", 1)
  val t2 = Test3(2, t)

  implicit val defaultStuff = Default.gen[Cabin]

  val finder = EasyFinder.gen[Cabin]

  val prog = for {
    _       <- creator5.createTable().right.get._3
    startInternal = System.nanoTime()
    id      <- saver.save(testPerson).fold(s => IO(s"Derived unfinished sql: $s"), identity)
    id2     <- saver.save(testPerson2).fold(s => IO(s"Derived unfinished sql: $s"), identity)
    id3     <- saver2.save(testCabin).fold(s => IO(s"Derived unfinished sql: $s"), identity)
    fount   <- finder.findById(id3)
    updated <- fount.map(cabin => updater.update(cabin.copy(temperature = 10.2, squareMeters = 24)).fold(s => IO(s"derived unfinished sql: $s"), identity)).getOrElse(IO(s"Not found"))
    fount2  <- finder.findById(id3)
    found   <- PostgresStuff.find(id.toInt)
    found2  <- PostgresStuff.find(id2.toInt)
    found3  <- PostgresStuff.find2(id2.toInt)
    allPets <- PostgresStuff.getAllPets()
  endInternal = System.nanoTime()
  } yield (found, found2, found3, id3, allPets, fount, updated, fount2, endInternal-startInternal nanos)

  val start = System.nanoTime();
  val res = prog.unsafeRunSync()
  val end = System.nanoTime();

  val totalTime = end-start nanos;

  println(s"Total time: ${totalTime.toMillis}")

  println(res)
  println(res._7)
  println(res._3)
  println(res._6)
  println(res._8)
  println(s"Total time internal: ${res._9.toMillis}")
}
