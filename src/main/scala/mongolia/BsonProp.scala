package mongolia

import BsonCodecs.{DblCdc, IntCdc, LongCdc}
import scuff.Codec

class BsonProp(val key: String, value: BsonValue) {
  def raw = if (value == null) null else value.raw
  override def toString = key concat " : " concat String.valueOf(value.raw)
}
sealed trait BsonNumProp extends BsonProp
final class BsonIntProp(key: String, intValue: Int) extends BsonProp(key, IntCdc.encode(intValue)) with BsonNumProp
final class BsonLngProp(key: String, lngValue: Long) extends BsonProp(key, LongCdc.encode(lngValue)) with BsonNumProp
final class BsonDblProp(key: String, dblValue: Double) extends BsonProp(key, DblCdc.encode(dblValue)) with BsonNumProp

final class Assignment(key: String) {
  def :=(value: BsonValue) = new BsonProp(key, value)
  def :=[T](value: T)(implicit codec: Codec[T, BsonValue]) = new BsonProp(key, codec.encode(value))
  def :=(value: Int) = new BsonIntProp(key, value)
  def :=(value: Long) = new BsonLngProp(key, value)
  def :=(value: Double) = new BsonDblProp(key, value)
}
