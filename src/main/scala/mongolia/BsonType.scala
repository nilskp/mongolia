package mongolia

sealed abstract class BsonType(val typeNumber: Byte)

object BsonType {
  case object Double extends BsonType(1)
  case object String extends BsonType(2)
  case object Object extends BsonType(3)
  case object Array extends BsonType(4)
  case object Binary extends BsonType(5)
  case object OID extends BsonType(7)
  case object Boolean extends BsonType(8)
  case object Date extends BsonType(9)
  case object Null extends BsonType(10)
  case object Regexp extends BsonType(11)
  case object Javascript extends BsonType(13)
  case object Symbol extends BsonType(14)
  case object JavascriptWithScope extends BsonType(15)
  case object Int extends BsonType(16)
  case object MongoTimestamp extends BsonType(17)
  case object Long extends BsonType(18)
  case object MinKey extends BsonType(-1)
  case object MaxKey extends BsonType(127)
}
