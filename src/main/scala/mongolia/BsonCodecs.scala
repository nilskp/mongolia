package mongolia

import com.mongodb.{ DBObject, BasicDBList }
import org.bson.types._
import scuff._
import java.util.{ Date, UUID, TimeZone, Locale }
import scala.reflect.ClassTag

object BsonCodecs {

  implicit val NoneCdc = new Codec[None.type, BsonValue] {
    val NullBsonValue = new BsonValue { def raw = null }
    def encode(a: None.type): BsonValue = NullBsonValue
    def decode(b: BsonValue) = null
  }
  implicit val RgxCdc = new Codec[java.util.regex.Pattern, BsonValue] {
    def encode(rgx: java.util.regex.Pattern): BsonValue = new Value(rgx): BsonValue
    def decode(b: BsonValue) = java.util.regex.Pattern.compile(String.valueOf(b.raw))
  }
  implicit val SRgxCdc = new Codec[scala.util.matching.Regex, BsonValue] {
    def encode(a: scala.util.matching.Regex): BsonValue = RgxCdc.encode(a.pattern)
    def decode(b: BsonValue) = new scala.util.matching.Regex(String.valueOf(b.raw))
  }
  implicit val StrCdc = new Codec[String, BsonValue] {
    def encode(a: String): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case _: DBObject => throw new RuntimeException("Cannot coerce DBObject into String")
      case _ => String.valueOf(b.raw)
    }
  }
  implicit val PropCdc = new Codec[BsonProp, BsonValue] {
    def encode(a: BsonProp): BsonValue = prop2obj(a)
    def decode(b: BsonValue) = b.raw match {
      case dbo: DBObject =>
        val keys = dbo.keySet()
        if (keys.size == 1) {
          val name = keys.iterator.next
          name := new Value(dbo.get(name))
        } else {
          throw new RuntimeException("Cannot extract single BsonProp when %d are available: %s".format(keys.size, keys))
        }
      case _ => throw new RuntimeException("Cannot coerce %s into BsonProp".format(b.raw.getClass.getName))
    }
  }
  implicit val ClzCdc = new Codec[Class[_], BsonValue] {
    def encode(a: Class[_]) = StrCdc.encode(a.getName)
    def decode(b: BsonValue): Class[_] = Class.forName(StrCdc.decode(b))
  }

  implicit val IntPropCdc = new Codec[BsonIntProp, BsonValue] {
    def encode(a: BsonIntProp) = PropCdc.encode(a)
    def decode(b: BsonValue) = {
      val prop = PropCdc.decode(b)
      new BsonIntProp(prop.key, prop.raw.asInstanceOf[Int])
    }
  }
  implicit val DblPropCdc = new Codec[BsonDblProp, BsonValue] {
    def encode(a: BsonDblProp) = PropCdc.encode(a)
    def decode(b: BsonValue) = {
      val prop = PropCdc.decode(b)
      new BsonDblProp(prop.key, prop.raw.asInstanceOf[Double])
    }
  }
  implicit val LngPropCdc = new Codec[BsonLngProp, BsonValue] {
    def encode(a: BsonLngProp) = PropCdc.encode(a)
    def decode(b: BsonValue) = {
      val prop = PropCdc.decode(b)
      new BsonLngProp(prop.key, prop.raw.asInstanceOf[Long])
    }
  }
  implicit val IntCdc = new Codec[Int, BsonValue] {
    def encode(a: Int): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case n: Number => n.intValue
      case str: String => str.toInt
      case _ => org.bson.BSON.toInt(b.raw)
    }
  }
  implicit val JIntCdc = new Codec[Integer, BsonValue] {
    def encode(a: Integer): BsonValue = new Value(a)
    def decode(b: BsonValue): Integer = b.raw match {
      case n: Number => n.intValue
      case str: String => str.toInt
      case _ => org.bson.BSON.toInt(b.raw)
    }
  }
  implicit val LongCdc = new Codec[Long, BsonValue] {
    def encode(a: Long): BsonValue = new Value(a)
    def decode(b: BsonValue): Long = b.raw match {
      case n: Number => n.longValue()
      case d: java.util.Date => d.getTime
      case s: String => s.toLong
      case _ => throwCoercionException(b.raw, "Long")
    }
  }
  implicit val JLongCdc = new Codec[java.lang.Long, BsonValue] {
    def encode(a: java.lang.Long): BsonValue = new Value(a)
    def decode(b: BsonValue): java.lang.Long = b.raw match {
      case n: Number => n.longValue()
      case d: java.util.Date => d.getTime
      case s: String => s.toLong
      case _ => throwCoercionException(b.raw, "Long")
    }
  }
  implicit val DblCdc = new Codec[Double, BsonValue] {
    def encode(a: Double): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case n: Number => n.doubleValue()
      case s: String => s.toDouble
      case _ => throwCoercionException(b.raw, "Double")
    }
  }
  implicit val JDblCdc = new Codec[java.lang.Double, BsonValue] {
    def encode(a: java.lang.Double): BsonValue = new Value(a)
    def decode(b: BsonValue): java.lang.Double = b.raw match {
      case n: Number => n.doubleValue()
      case s: String => s.toDouble
      case _ => throwCoercionException(b.raw, "Double")
    }
  }
  implicit val FltCdc = new Codec[Float, BsonValue] {
    def encode(a: Float): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case n: Number => n.floatValue()
      case s: String => s.toFloat
      case _ => throwCoercionException(b.raw, "Float")
    }
  }
  implicit val ShrtCdc = new Codec[Short, BsonValue] {
    def encode(a: Short): BsonValue = new Value(a)
    def decode(b: BsonValue): Short = b.raw match {
      case n: Number => n.shortValue()
      case s: String => s.toShort
      case _ => throwCoercionException(b.raw, "Short")
    }
  }
  implicit val ByteCdc = new Codec[Byte, BsonValue] {
    def encode(a: Byte): BsonValue = new Value(a)
    def decode(b: BsonValue): Byte = b.raw match {
      case n: Number => n.byteValue
      case s: String => s.toByte
      case _ => throwCoercionException(b.raw, "Short")
    }
  }
  implicit val BoolCdc = new Codec[Boolean, BsonValue] {
    val TrueStrings = Set("1", "true", "on", "yes", "y")
    def encode(a: Boolean): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case b: java.lang.Boolean => b.booleanValue
      case i: Int => IntCdc.decode(b) != 0
      case s: String => TrueStrings.contains(s.toLowerCase)
      case _ => throwCoercionException(b.raw, "Boolean")
    }
  }
  implicit val BACdc = new Codec[Array[Byte], BsonValue] {
    def encode(a: Array[Byte]): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case arr: Array[Byte] => arr
      case bin: Binary => bin.getData
      case _ => throwCoercionException(b.raw, "Array[Byte]")
    }
  }
  implicit val IACdc = new Codec[Array[Int], BsonValue] {
    def encode(a: Array[Int]): BsonValue = new Value(arr(a: _*))
    def decode(b: BsonValue): Array[Int] = b.raw match {
      case arr: Array[Int] => arr
      case list: java.util.List[_] =>
        val array = new Array[Int](list.size)
        val iter = list.iterator()
        var idx = 0
        while (iter.hasNext) {
          iter.next match {
            case n: Number => array(idx) = n.intValue
            case v => throwCoercionException(v, "Int")
          }
          idx += 1
        }
        array
      case _ => throwCoercionException(b.raw, "Array[Int]")
    }
  }
  implicit val LACdc = new Codec[Array[Long], BsonValue] {
    def encode(a: Array[Long]): BsonValue = new Value(arr(a: _*))
    def decode(b: BsonValue): Array[Long] = b.raw match {
      case arr: Array[Long] => arr
      case list: java.util.List[_] =>
        val array = new Array[Long](list.size)
        val iter = list.iterator()
        var idx = 0
        while (iter.hasNext) {
          iter.next match {
            case n: Number => array(idx) = n.longValue
            case v => throwCoercionException(v, "Long")
          }
          idx += 1
        }
        array
      case _ => throwCoercionException(b.raw, "Array[Long]")
    }
  }
  implicit val DACdc = new Codec[Array[Double], BsonValue] {
    def encode(a: Array[Double]): BsonValue = new Value(arr(a: _*))
    def decode(b: BsonValue): Array[Double] = b.raw match {
      case arr: Array[Double] => arr
      case list: java.util.List[_] =>
        val array = new Array[Double](list.size)
        val iter = list.iterator()
        var idx = 0
        while (iter.hasNext) {
          iter.next match {
            case n: Number => array(idx) = n.doubleValue
            case v => throwCoercionException(v, "Double")
          }
          idx += 1
        }
        array
      case _ => throwCoercionException(b.raw, "Array[Double]")
    }
  }
  implicit val BinCdc = new Codec[Binary, BsonValue] {
    def encode(a: Binary): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case arr: Array[Byte] => new Binary(arr)
      case bin: Binary => bin
      case _ => throwCoercionException(b.raw, "Binary")
    }
  }
  implicit val DateCdc = new Codec[Date, BsonValue] {
    def encode(a: Date): BsonValue = new Value(a)
    def decode(b: BsonValue): Date = b.raw match {
      case n: Number => new Date(n.longValue)
      case ts: Timestamp => new Date(ts.asMillis)
      case d: Date => d
      case oid: ObjectId => oid.getDate
      case _ => throwCoercionException(b.raw, "Date")
    }
  }
  implicit val TsCdc = new Codec[Timestamp, BsonValue] {
    def encode(a: Timestamp): BsonValue = new Value(a)
    def decode(b: BsonValue) = b.raw match {
      case n: Number => new Timestamp(n.longValue)
      case ts: Timestamp => ts
      case d: Date => new Timestamp(d)
      case oid: ObjectId => new Timestamp(oid.getDate.getTime)
      case str: String => Timestamp.parseISO(str).get
      case _ => throwCoercionException(b.raw, "Timestamp")
    }
  }
  implicit val OIDCdc = new Codec[ObjectId, BsonValue] {
    def encode(a: ObjectId): BsonValue = new Value(a)
    def decode(b: BsonValue) = decode(b.raw)
    def decode(any: Any) = any match {
      case oid: ObjectId => oid
      case arr: Array[Byte] if arr.length == 12 => new ObjectId(arr)
      case str: String if ObjectId.isValid(str) => new ObjectId(str)
      case _ => throwCoercionException(any, "ObjectId")
    }
  }
  implicit val UUIDCdc = new Codec[UUID, BsonValue] {
    def encode(uuid: UUID): BsonValue = {
      val bb = java.nio.ByteBuffer.allocate(16)
      bb.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)
      new Value(new Binary(4, bb.array))
    }
    def decode(b: BsonValue) = b.raw match {
      case u: UUID => u
      case b: Binary if b.getType == 4 => binaryType4ToUUID(b.getData)
      case a: Array[Byte] if a.length == 16 => binaryType4ToUUID(a)
      case s: String if s.length == 36 => UUID.fromString(s)
      case _ => throwCoercionException(b.raw, "UUID")
    }
  }
  implicit val PwdCdc = new Codec[Password, BsonValue] {
    def encode(a: Password): BsonValue = {
      val dbo = obj("digest" := a.digest, "algo" := a.algorithm)
      if (a.salt.length > 0) dbo.add("salt" := a.salt)
      if (a.workFactor > 1) dbo.add("work" := a.workFactor)
      new Value(dbo)
    }
    def decode(b: BsonValue) = b.raw match {
      case obj: DBObject =>
        val dbo = enrich(obj)
        new Password(dbo("digest").as[Array[Byte]], dbo("algo").as[String], dbo("salt").opt[Array[Byte]].getOrElse(Array.empty), dbo("work").opt[Int].getOrElse(1))
      case _ => throwCoercionException(b.raw, "Password")
    }
  }
  implicit val EmlCdc = new Codec[EmailAddress, BsonValue] {
    def encode(a: EmailAddress): BsonValue = new Value(a.toString)
    def decode(b: BsonValue) = new EmailAddress(String.valueOf(b.raw))
  }
  implicit val UrlCdc = new Codec[java.net.URL, BsonValue] {
    def encode(a: java.net.URL): BsonValue = new Value(a.toString)
    def decode(b: BsonValue) = new java.net.URL(String.valueOf(b.raw))
  }
  implicit val UriCdc = new Codec[java.net.URI, BsonValue] {
    def encode(a: java.net.URI): BsonValue = new Value(a.toString)
    def decode(b: BsonValue) = new java.net.URI(String.valueOf(b.raw))
  }
  implicit val TzCdc = new Codec[TimeZone, BsonValue] {
    def encode(a: TimeZone): BsonValue = new Value(a.getID)
    def decode(b: BsonValue) = TimeZone.getTimeZone(String.valueOf(b.raw))
  }
  implicit val BDCdc = new Codec[BigDecimal, BsonValue] {
    def encode(a: BigDecimal): BsonValue = new Value(a.toString)
    def decode(b: BsonValue): BigDecimal = b.raw match {
      case bd: java.math.BigDecimal => BigDecimal(bd)
      case s: String => BigDecimal(s)
      case d: Double => BigDecimal(d)
      case i: Int => BigDecimal(i)
      case l: Long => BigDecimal(l)
      case _ => throwCoercionException(b.raw, "BigDecimal")
    }
  }
  implicit def EnumCdc[T <: Enum[T]](implicit tag: ClassTag[T]) = new Codec[T, BsonValue] {
    private def constants = tag.runtimeClass.getEnumConstants().asInstanceOf[Array[T]]
    def encode(e: T): BsonValue = new Value(e.ordinal)
    def decode(b: BsonValue): T = b.raw match {
      case i: Int => constants(i)
      case name: String => findByName(name)
      case n: Number => constants(n.intValue)
      case _ => throwCoercionException(b.raw, tag.runtimeClass.getName)
    }
    @annotation.tailrec
    private def findByName(name: String, idx: Int = 0): T = {
      if (idx == constants.length) {
        throwCoercionException(name, tag.runtimeClass.getName)
      } else {
        val enum = constants(idx)
        if (enum.name == name) {
          enum
        } else {
          findByName(name, idx + 1)
        }
      }
    }
  }

  implicit def SEnumCdc[T <: Enumeration](implicit enum: T) = new Codec[T#Value, BsonValue] {
    def encode(e: T#Value): BsonValue = new Value(e.id)
    def decode(b: BsonValue): T#Value = b.raw match {
      case i: Int => enum(i)
      case name: String => enum.withName(name)
      case n: Number => enum(n.intValue)
      case _ => throwCoercionException(b.raw, enum.getClass.getName)
    }
  }

  implicit def GeoPointCdc: Codec[GeoPoint, BsonValue] = GeoCdc
  private[this] val GeoCdc = new Codec[GeoPoint, BsonValue] {
    def encode(a: GeoPoint): BsonValue = {
      val dbo = geo2Dbo(a)
      if (a.radius > 0f) dbo.add("radius" := a.radius)
      dbo: BsonValue
    }
    def decode(b: BsonValue): GeoPoint = b.raw match {
      case coords: java.util.List[Number] if coords.size == 2 =>
        new GeoPoint(coords.get(1).doubleValue, coords.get(0).doubleValue)
      case dbo: DBObject =>
        val coords = dbo.getAs[java.util.List[Number]]("coordinates")
        val radius: Float = dbo("radius").opt[Float].getOrElse(0f)
        new GeoPoint(coords.get(1).doubleValue, coords.get(0).doubleValue, radius)
      case Array(longitude: Double, latitude: Double) =>
        new GeoPoint(latitude = latitude, longitude = longitude)
      case _ => throwCoercionException(b.raw, "GeoPoint")
    }
  }

  implicit val LocCdc = new Codec[Locale, BsonValue] {
    def encode(a: Locale): BsonValue = new Value(a.toLanguageTag)
    def decode(b: BsonValue) = Locale.forLanguageTag(String.valueOf(b.raw))
  }
  implicit def OptCdc[T](implicit codec: Codec[T, BsonValue]) = new Codec[Option[T], BsonValue] {
    def encode(a: Option[T]) = a match {
      case Some(t) => codec.encode(t)
      case _ => null
    }
    def decode(b: BsonValue) = b.raw match {
      case null => None
      case _ => Some(codec.decode(b))
    }
  }
  implicit def DBObjectCdc: Codec[DBObject, BsonValue] = DboCdc
  private[this] val DboCdc = new Codec[DBObject, BsonValue] {
    def encode(a: DBObject): BsonValue = a match {
      case l: BsonList => l
      case _ => enrich(a)
    }
    def decode(b: BsonValue): DBObject = b.raw match {
      case list: BasicDBList => list
      case list: org.bson.LazyDBList => list
      case dbo: DBObject => dbo.enrich: DBObject
      case _ => throwCoercionException(b.raw, "DBObject")
    }
  }
  implicit def BsonObjectCdc: Codec[BsonObject, BsonValue] = RDboCdc
  private[this] val RDboCdc = new Codec[BsonObject, BsonValue] {
    def encode(a: BsonObject): BsonValue = a
    def decode(b: BsonValue): BsonObject = b.raw match {
      case dbo: BsonObject => dbo
      case dbo: DBObject => dbo.enrich
      case _ => throwCoercionException(b.raw, "BsonObject")
    }
  }

  implicit def MapCdc[T](implicit codec: Codec[T, BsonValue]) = new Codec[Map[String, T], BsonValue] {
    def encode(a: Map[String, T]): BsonValue = {
      val dbo = new BsonObject
      dbo.add(a)
      dbo
    }
    private def toValue(value: Any): T = if (value == null) null.asInstanceOf[T] else codec.decode(new Value(value))
    def decode(b: BsonValue): Map[String, T] = {
      var map: Map[String, T] = Map.empty
      b.raw match {
        case jmap: java.util.Map[_, _] =>
          val iter = jmap.asInstanceOf[java.util.Map[String, Any]].entrySet.iterator
          while (iter.hasNext) {
            val entry = iter.next
            map += (entry.getKey -> toValue(entry.getValue))
          }
        case dbo: DBObject =>
          val keys = dbo.keys.iterator
          while (keys.hasNext) {
            val key = keys.next
            map += key -> toValue(dbo.get(key))
          }
      }
      map
    }
  }

  implicit def ArrayListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[Array[T], BsonValue] {
    def encode(a: Array[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): Array[T] = any2Array(b.raw)
  }
  implicit def IdxSeqListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[IndexedSeq[T], BsonValue] {
    def encode(a: IndexedSeq[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): IndexedSeq[T] = any2Array(b.raw)(codec, tag)
  }
  implicit def iIdxSeqListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[collection.immutable.IndexedSeq[T], BsonValue] {
    def encode(a: collection.immutable.IndexedSeq[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): collection.immutable.IndexedSeq[T] = Vector(any2Array(b.raw)(codec, tag): _*)
  }
  implicit def SeqListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[Seq[T], BsonValue] {
    def encode(a: Seq[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): Seq[T] = any2Array(b.raw)(codec, tag)
  }
  implicit def ListListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[List[T], BsonValue] {
    def encode(a: List[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): List[T] = any2Array(b.raw)(codec, tag).toList
  }
  implicit def IterListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[Iterable[T], BsonValue] {
    def encode(a: Iterable[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): Iterable[T] = any2Array(b.raw)(codec, tag)
  }
  implicit def TravListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[Traversable[T], BsonValue] {
    def encode(a: Traversable[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): Traversable[T] = any2Array(b.raw)(codec, tag)
  }
  implicit def SetListCdc[T](implicit codec: Codec[T, BsonValue], tag: ClassTag[T]) = new Codec[Set[T], BsonValue] {
    def encode(a: Set[T]): BsonValue = seq2bsonlist(a)
    def decode(b: BsonValue): Set[T] = any2Array(b.raw)(codec, tag).toSet
  }

}
