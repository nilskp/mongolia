package mongolia

import com.mongodb.DBObject
import org.bson.types.ObjectId
import java.util.UUID
import org.bson.types.Binary
import scuff.Base64
import scuff.concurrent.ResourcePool

object Json {

  private[this] val serializers = new ResourcePool(com.mongodb.util.JSONSerializers.getStrict)

  private[this] val base64 = Base64.Custom('+', '/', withPadding = true, paddingChar = '=')

  /**
   * Faster, more compact serialization,
   * and more importantly *correct* JSON, i.e.
   * `NaN` is translated to `null`.
   */
  def toJson(dbo: DBObject, sb: java.lang.StringBuilder = new java.lang.StringBuilder(128)): java.lang.StringBuilder = {
      def appendList(list: collection.GenTraversableOnce[_]) {
        sb append '['
        val i = list.toIterator
        var first = true
        while (i.hasNext) {
          if (first)
            first = false
          else
            sb append ','
          append(i.next)
        }
        sb append ']'
      }
      def appendMap(map: java.util.Map[_, _]) {
        import language.existentials
        sb append '{'
        val i = map.entrySet.iterator
        var first = true
        while (i.hasNext) {
          val entry = i.next
          if (first)
            first = false
          else
            sb append ','
          appendString(String.valueOf(entry.getKey))
          sb append ':'
          append(entry.getValue)
        }
        sb append '}'
      }
      def appendString(str: String) {
        sb append "\""
        var i = 0
        while (i < str.length) {
          str.charAt(i) match {
            case '\\' => sb append "\\\\"
            case '"' => sb append "\\\""
            case '\n' => sb append "\\n"
            case '\r' => sb append "\\r"
            case '\t' => sb append "\\t"
            case '\b' => sb append "\\b"
            case c if c < 32 => // Ignore
            case c => sb append c
          }
          i += 1
        }
        sb append "\""
      }
      def append(any: Any): Unit = any match {
        case null => sb append "null"
        case s: String => appendString(s)
        case d: Double => if (java.lang.Double.isNaN(d) || java.lang.Double.isInfinite(d)) sb append "null" else sb append d
        case i: Int => sb append i
        case l: Long => sb append l
        case b: Boolean => sb append b
        case d: java.util.Date => sb append "{\"$date\":" append d.getTime append '}'
        case id: ObjectId => sb append "{\"$oid\":\"" append id.toString append "\"}"
        case u: UUID => sb append "{\"$uuid\":\"" append u.toString append "\"}"
        case a: Array[AnyRef] => appendList(a)
        case b: Binary => if (b.getType == 4) {
          sb append "{\"$uuid\":\"" append binaryType4ToUUID(b.getData).toString append "\"}"
        } else {
          sb append "{\"$binary\":\"" append base64.encode(b.getData) append "\",\"$type\":" append b.getType append '}'
        }
        case r: BsonObject => appendRef(r.impoverish)
        case f: Float => if (java.lang.Float.isNaN(f) || java.lang.Float.isInfinite(f)) sb append "null" else sb append f
        case _ => appendRef(any.asInstanceOf[AnyRef])
      }
      def appendRef(anyRef: AnyRef): Unit = {
        import collection.JavaConverters._
        anyRef match {
          case m: java.util.Map[_, _] => appendMap(m)
          case l: java.lang.Iterable[_] => appendList(l.asScala)
          case t: collection.GenTraversableOnce[_] => appendList(t)
          case _ => serializers.borrow(_.serialize(anyRef, sb))
        }
      }

    append(dbo)
    sb
  }

  def parse(json: String): Any = com.mongodb.util.JSON.parse(json) match {
    case dbo: DBObject => enrich(dbo)
    case a => a
  }
  def parseObject(json: String): Option[DBObject] = json match {
    case null => None
    case json =>
      parse(json) match {
        case dbo: DBObject => Some(dbo)
        case _ => None
      }
  }

}
