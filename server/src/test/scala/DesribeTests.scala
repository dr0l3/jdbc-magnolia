import java.util.UUID

import SqlAnnotations.{fieldName, id}
import org.scalatest.{Matchers, WordSpec}
class DesribeTests extends WordSpec with Matchers {
  "describe method" when {
    "Simple case class" should {
      "contain the id type" in {
        case class A(@id id: UUID, first: String, second: Int)
        val ops = RepoOps.gen[A]
        val description = ops.describe(false, false, TableName(""), null, false, ColumnName(""))

        val desc = description match {
          case d: TableDescRegular => d
          case _ => throw new RuntimeException("Should be table desc regular")
        }

        desc.tableName.name shouldBe "a"
        desc.joinOnFind shouldBe false
        desc.isSubtypeTable shouldBe false
        desc.referencesConstraint shouldBe None
        desc.idColumn.columnName.name shouldBe "id"
        desc.idColumn.idValueDesc.idType shouldBe IdType.UUID

        desc.additionalColumns.size shouldBe 2
        desc.additionalColumns.head.columnName.name shouldBe "first"
        desc.additionalColumns.head.regularValue match {
          case RegularLeaf(dataType, tableName, columnName)      =>
            dataType match {
              case Text  => "Success"
            }
            tableName.name shouldBe "a"
            columnName.name shouldBe "first"
        }
        desc.additionalColumns(1).columnName.name shouldBe "second"
        desc.additionalColumns(1).regularValue match {
          case RegularLeaf(dataType, tableName, columnName)      =>
            dataType match {
              case idType: IdType     => idType match {
                case IdType.Integer   => "Success"
              }
            }
            tableName.name shouldBe "a"
            columnName.name shouldBe "second"
        }
      }
    }

    "Lists" should {
      "a single list with primitive values" in {
        case class A(@id id: UUID, list: List[String])
        val ops = RepoOps.gen[A]
        val description = ops.describe(false, false, TableName(""), null, false, ColumnName(""))

        val desc = description match {
          case d: TableDescRegular => d
          case _ => throw new RuntimeException("Should be table desc regular")
        }

        val listColumn = desc.additionalColumns.head

        listColumn.columnName.name shouldBe "list"

        listColumn.regularValue match {
          case TableDescSeqType(tableName, idColumn, entityDesc) =>
            tableName.name shouldBe "a_list"
            idColumn.columnName.name shouldBe desc.idColumn.columnName.name
            idColumn.idValueDesc.idType shouldBe desc.idColumn.idValueDesc.idType
            entityDesc match {
              case RegularLeaf(dataType, leafTableName, columnName)      =>
                dataType match {
                  case Text  => "Success"
                }

                leafTableName.name shouldBe tableName.name
                columnName.name shouldBe "list"
            }
        }

      }

      "a single list with case class values" in {
        case class B(@id id: UUID, name: String, age: Int)
        case class A(@id id: UUID, list: List[B])
        val ops = RepoOps.gen[A]
        val description = ops.describe(false, false, TableName(""), null, false, ColumnName(""))

        val desc = description match {
          case d: TableDescRegular => d
          case _ => throw new RuntimeException("Should be table desc regular")
        }

        val listColumn = desc.additionalColumns.head

        listColumn.columnName.name shouldBe "list"

        listColumn.regularValue match {
          case TableDescSeqType(tableName, idColumn, entityDesc) =>
            tableName.name shouldBe "a_list"
            idColumn.columnName.name shouldBe desc.idColumn.columnName.name
            entityDesc match {
              case TableDescRegular(tableName, idColumn, additionalColumns, referencesConstraint, isSubtypeTable, joinOnFind) =>
                tableName.name shouldBe "b"
                idColumn.columnName.name shouldBe "id"
                idColumn.idValueDesc.idType match {
                  case IdType.UUID      => "Success"
                }

                additionalColumns.map(_.columnName.name) shouldBe Seq("name", "age")
                referencesConstraint.isDefined shouldBe false
                isSubtypeTable shouldBe false
                joinOnFind shouldBe true
            }
        }

      }
    }

    "Complex example" should {
      "describe" in {
        case class Book(@id @fieldName("book_id") bookId: UUID, title: String, published: Int)
        sealed trait Person
        case class Friend(@id id: UUID, name: String, daysFriends: Int, books: List[Book]) extends Person
        case class Stranger(@id id: UUID, distance: Double, streetName: List[String])      extends Person

        val ops = RepoOps.gen[Person]
        val description = ops.describe(false, false, TableName(""), null, false, ColumnName(""))

        description match {
          case TableDescSumType(tableName, idColumn, subtypeTableNameCol, subType, joinOnFind) =>
            tableName.name shouldBe "person"
            idColumn.columnName.name shouldBe "person_id"
            idColumn.idValueDesc.idType match {
              case IdType.UUID      => "Success"
            }

            subtypeTableNameCol.columnName.name shouldBe "person_type"
            subtypeTableNameCol.regularValue match {
              case RegularLeaf(dataType, leafTableName, columnName)      =>
                dataType match {
                  case Text  => "success"
                }

                leafTableName.name shouldBe tableName.name
                columnName.name shouldBe subtypeTableNameCol.columnName.name
            }

            joinOnFind shouldBe false

            val friendDesc = subType.head

            friendDesc.tableName.name shouldBe "friend"
            friendDesc.joinOnFind shouldBe true
            friendDesc.isSubtypeTable shouldBe true
            val booksColumn =  friendDesc.additionalColumns.find(_.columnName.name == "books").get

            booksColumn.columnName.name shouldBe "books"
            booksColumn.regularValue match {
              case TableDescSeqType(booksTableName, booksIdColumn, booksEntityDesc) =>
                booksTableName.name shouldBe "friend_books"
                booksIdColumn.columnName.name shouldBe "id"
                booksEntityDesc match {
                  case TableDescRegular(bookTableName, bookIdColumn, bookAdditionalColumns, bookReferencesConstraint, bookIsSubtypeTable, bookJoinOnFind) =>
                    bookTableName.name shouldBe "book"
                    bookIdColumn.columnName.name shouldBe "book_id"
                    bookAdditionalColumns.map(_.columnName.name) shouldBe Seq("title", "published")
                    bookReferencesConstraint.isDefined shouldBe false
                    bookIsSubtypeTable shouldBe false
                    bookJoinOnFind shouldBe true
                }

            }

            val strangerDesc = subType(1)

            strangerDesc.tableName .name shouldBe "stranger"
            strangerDesc.joinOnFind shouldBe true
            strangerDesc.isSubtypeTable shouldBe true
            val streetNameColumn = strangerDesc.additionalColumns.find(_.columnName.name == "streetName").get
            streetNameColumn.regularValue match {
              case TableDescSeqType(strangerTableName, strangerIdColumn, strangerEntityDesc) =>
                strangerTableName.name shouldBe "stranger_streetName"
                strangerIdColumn.columnName.name shouldBe strangerDesc.idColumn.columnName.name
                strangerEntityDesc match {
                  case RegularLeaf(streetNamedataType, streetNametableName, streetNamecolumnName)      =>
                    streetNamedataType match {
                      case Text  => "Success"
                    }
                    streetNametableName.name shouldBe strangerTableName.name
                    streetNamecolumnName.name shouldBe "streetName"
                }
            }
        }
      }
    }
  }

}
