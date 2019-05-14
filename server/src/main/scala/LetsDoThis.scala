import LetsDoThis.showStuff.{ name, secret }
import magnolia._
import shapeless._
import ops.hlist.Tupler

import scala.annotation.Annotation
import scala.language.experimental.macros

object LetsDoThis extends App {

  sealed class showStuff extends Annotation
  object showStuff {
    final case class name(n: String) extends showStuff
    final case class secret()        extends showStuff
  }

  trait Show[Out, T] { def show(value: T): Out }

  trait GenericShow[Out] {

    /**
     * the type constructor for new [[Show]] instances
     *
     *  The first parameter is fixed as `String`, and the second parameter varies generically.
     */
    type Typeclass[T] = Show[Out, T]

    def join(typeName: String, strings: Seq[String]): Out
    def prefix(s: String, out: Out): Out

    /**
     * creates a new [[Show]] instance by labelling and joining (with `mkString`) the result of
     *  showing each parameter, and prefixing it with the class name
     */
    def combine[T](ctx: CaseClass[Typeclass, T]): Show[Out, T] = { value =>
      if (ctx.isValueClass) {
        val param = ctx.parameters.head
        param.typeclass.show(param.dereference(value))
      } else {
        val paramStrings = ctx.parameters.map { param =>
          val label = param.annotations.collectFirst {
            case showStuff.name(n) => n
          }

          val attribStr = ""
          if (param.annotations.isEmpty) ""
          else {
            param.annotations.mkString("{", ", ", "}")
          }
          val hide = param.annotations.exists {
            case showStuff.secret() => true
            case _                  => false
          }

          val fieldVal = if (hide) "<hidden>" else param.typeclass.show(param.dereference(value))
          s"${label.getOrElse(param.label)}$attribStr=$fieldVal"
        }

        val anns          = ctx.annotations.filterNot(_.isInstanceOf[scala.SerialVersionUID])
        val annotationStr = if (anns.isEmpty) "" else anns.mkString("{", ",", "}")

        join(ctx.typeName.short + annotationStr, paramStrings)
      }
    }

    /**
     * choose which typeclass to use based on the subtype of the sealed trait
     * and prefix with the annotations as discovered on the subtype.
     */
    def dispatch[T](ctx: SealedTrait[Typeclass, T]): Show[Out, T] = (value: T) => {
      ctx.dispatch(value) { sub =>
        val anns          = sub.annotations.filterNot(_.isInstanceOf[scala.SerialVersionUID])
        val annotationStr = if (anns.isEmpty) "" else anns.mkString("[", ",", "]")
        prefix(annotationStr, sub.typeclass.show(sub.cast(value)))
      }
    }

    /** bind the Magnolia macro to this derivation object */
    implicit def gen[T]: Show[Out, T] = macro Magnolia.gen[T]
  }

  object Show extends GenericShow[String] {

    /** show typeclass for strings */
    implicit val string: Show[String, String] = (s: String) => s

    def join(typeName: String, params: Seq[String]): String =
      params.mkString(s"$typeName(", ",", ")")
    def prefix(s: String, out: String): String = s + out

    /** show typeclass for integers */
    implicit val int: Show[String, Int]      = (s: Int) => s.toString
    implicit val bool: Show[String, Boolean] = (b: Boolean) => b.toString

    /** show typeclass for sequences */
    implicit def seq[A](implicit A: Show[String, A]): Show[String, Seq[A]] =
      (as: Seq[A]) => as.map(A.show).mkString("[", ",", "]")

    implicit def list[A](implicit A: Show[String, A]): Show[String, List[A]] =
      (as: List[A]) => {
        as.map(A.show).mkString("[", ",", "]")
      }
  }

  trait TypeNameInfo[T] { def name: TypeName }

  object TypeNameInfo {
    type Typeclass[T] = TypeNameInfo[T]
    def combine[T](ctx: CaseClass[TypeNameInfo, T]): TypeNameInfo[T] =
      new TypeNameInfo[T] { def name: TypeName = ctx.typeName }

    def dispatch[T](ctx: SealedTrait[TypeNameInfo, T]): TypeNameInfo[T] =
      new TypeNameInfo[T] { def name: TypeName = ctx.typeName }

    def fallback[T]: TypeNameInfo[T] =
      new TypeNameInfo[T] { def name: TypeName = TypeName("", "Unknown Type") }

    implicit def gen[T]: TypeNameInfo[T] = macro Magnolia.gen[T]
  }

  import magnolia._, mercator._
  import scala.language.experimental.macros

  /** typeclass for providing a default value for a particular type */
  trait Default[T] { def default: Either[String, T] }

  /** companion object and derivation object for [[Default]] */
  object Default {

    type Typeclass[T] = Default[T]

    /**
     * constructs a default for each parameter, using the constructor default (if provided),
     *  otherwise using a typeclass-provided default
     */
    def combine[T](ctx: CaseClass[Default, T]): Default[T] = new Default[T] {
      def default = ctx.constructMonadic { param =>
        param.default match {
          case Some(arg) => Right(arg)
          case None      => param.typeclass.default
        }
      }
    }

    /** chooses which subtype to delegate to */
    def dispatch[T](ctx: SealedTrait[Default, T])(): Default[T] = new Default[T] {
      def default = ctx.subtypes.headOption match {
        case Some(sub) => sub.typeclass.default
        case None      => Left("no subtypes")
      }
    }

    /** default value for a string; the empty string */
    implicit val string: Default[String] = new Default[String] { def default = Right("") }

    /** default value for ints; 0 */
    implicit val int: Default[Int] = new Default[Int] { def default = Right(0) }

    /** oh, no, there is no default Boolean... whatever will we do? */
    implicit val boolean: Default[Boolean] = new Default[Boolean] { def default = Left("truth is a lie") }

    /** oh, no, there is no default Boolean... whatever will we do? */
    implicit val double: Default[Double] = new Default[Double] { def default = Right(0.0) }

    /** default value for sequences; the empty sequence */
    implicit def seq[A]: Default[Seq[A]] = new Typeclass[Seq[A]] { def default = Right(Seq.empty) }

    /** generates default instances of [[Default]] for case classes and sealed traits */
    implicit def gen[T]: Default[T] = macro Magnolia.gen[T]
  }

  sealed trait Stuff
  case class A(@name("hehe") s: String, @name("omg") i: Int, @secret() b: Boolean) extends Stuff
  case class B(s2: String, a: Stuff)                                               extends Stuff
  case class C(a: A, b: B, meh: List[String])                                      extends Stuff

  val c = C(A("hehe", 31, false), B("omg", A("meh", 32, true)), List("hehe", "meh"))

  println(implicitly[Show[String, C]].show(c))
  println(implicitly[TypeNameInfo[C]].name)
}
