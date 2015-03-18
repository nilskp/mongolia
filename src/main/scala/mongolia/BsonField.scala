package mongolia

import com.mongodb.DBObject
import scuff.Codec

object BsonField {
  def apply(obj: Any, from: DBObject = null, key: String = null): BsonField = obj match {
    case null => if (from == null || from.containsField(key)) new Null(Option(key)) else new Undefined(Option(key))
    case obj => new Value(obj)
  }
  def apply(obj: DBObject, key: String): BsonField = apply(obj.get(key), obj, key)
}

sealed trait BsonField {
  def opt[T](implicit codec: Codec[T, BsonValue]): Option[T]
  def as[T](implicit codec: Codec[T, BsonValue]): T
  def asSeq[T](implicit codec: Codec[T, BsonValue]): IndexedSeq[T]
  def asSeqOfOption[T](implicit codec: Codec[T, BsonValue]): IndexedSeq[Option[T]]
  def asList[T](implicit codec: Codec[T, BsonValue]): List[T]
}
final class Null private[mongolia] (name: Option[String]) extends BsonField {
  def opt[T](implicit codec: Codec[T, BsonValue]) = None
  def as[T](implicit codec: Codec[T, BsonValue]): T = name match {
    case None => throw new UnavailableValueException("<n/a>")("Field value is null")
    case Some(name) => throw new UnavailableValueException(name)(s"""Field "$name" is null""")
  }
  def asSeqOfOption[T](implicit codec: Codec[T, BsonValue]) = IndexedSeq.empty
  def asSeq[T](implicit codec: Codec[T, BsonValue]) = IndexedSeq.empty
  def asList[T](implicit codec: Codec[T, BsonValue]) = Nil
}
final class Undefined private[mongolia] (name: Option[String]) extends BsonField {
  def opt[T](implicit codec: Codec[T, BsonValue]) = None
  def as[T](implicit codec: Codec[T, BsonValue]): T = name match {
    case None => throw new UnavailableValueException("<n/a>")("Field does not exist")
    case Some(name) => throw new UnavailableValueException(name)(s"""Field "$name" does not exist""")
  }
  def asSeqOfOption[T](implicit codec: Codec[T, BsonValue]) = IndexedSeq.empty
  def asSeq[T](implicit codec: Codec[T, BsonValue]) = IndexedSeq.empty
  def asList[T](implicit codec: Codec[T, BsonValue]) = Nil
}
final class Value private[mongolia] (val raw: Any) extends BsonField with BsonValue {
  override def toString() = "%s = %s".format(raw.getClass.getName, raw)
  def opt[T](implicit codec: Codec[T, BsonValue]): Option[T] = Option(codec.decode(this))
  def as[T](implicit codec: Codec[T, BsonValue]): T = codec.decode(this)
  def asSeqOfOption[T](implicit codec: Codec[T, BsonValue]): IndexedSeq[Option[T]] = {
    val list: Iterable[_] = anyToIterable(raw)
    val array = new Array[Option[T]](list.size)
    var i = 0
    val iter = list.iterator
    while (i < array.length) {
      array(i) = iter.next match {
        case null => None
        case a => Some(codec.decode(new Value(a)))
      }
      i += 1
    }
    array
  }
  def asSeq[T](implicit codec: Codec[T, BsonValue]): IndexedSeq[T] = {
    val list: Iterable[_] = anyToIterable(raw)
    val array = new Array[Any](list.size)
    var i = 0
    val iter = list.iterator
    while (i < array.length) {
      iter.next match {
        case null => // Ignore
        case a => array(i) = codec.decode(new Value(a))
      }
      i += 1
    }
    array.toIndexedSeq.asInstanceOf[IndexedSeq[T]]
  }
  def asList[T](implicit codec: Codec[T, BsonValue]) = {
    val iterable: Iterable[_] = anyToIterable(raw)
    var list: List[T] = Nil
    var i = 0
    val iter = iterable.iterator
    while (iter.hasNext) {
      iter.next match {
        case null => // Ignore
        case a => list ::= codec.decode(new Value(a))
      }
    }
    list.reverse
  }
}
