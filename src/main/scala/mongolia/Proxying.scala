package mongolia

import scuff._
import com.mongodb.DBObject
import org.bson.types.ObjectId
import java.util.{Date, UUID, Locale, TimeZone}
import scala.reflect._

private[mongolia] object Proxying {

  import BsonCodecs._

  private val DefaultProxyMapping: Map[Class[_], Codec[_, BsonValue]] = Map(
    classOf[String] -> StrCdc,
    classOf[Double] -> DblCdc,
    classOf[java.lang.Double] -> DblCdc,
    classOf[Float] -> FltCdc,
    classOf[java.lang.Float] -> FltCdc,
    classOf[Long] -> LongCdc,
    classOf[java.lang.Long] -> LongCdc,
    classOf[Int] -> IntCdc,
    classOf[java.lang.Integer] -> IntCdc,
    classOf[Byte] -> ByteCdc,
    classOf[java.lang.Byte] -> ByteCdc,
    classOf[Boolean] -> BoolCdc,
    classOf[java.lang.Boolean] -> BoolCdc,
    classOf[Short] -> ShrtCdc,
    classOf[java.lang.Short] -> ShrtCdc,
    classOf[UUID] -> UUIDCdc,
    classOf[BigDecimal] -> BDCdc,
    classOf[ObjectId] -> OIDCdc,
    classOf[Array[Byte]] -> BACdc,
    classOf[Array[Int]] -> IACdc,
    classOf[Array[Long]] -> LACdc,
    classOf[Array[Double]] -> DACdc,
    classOf[Array[String]] -> ArrayListCdc[String],
    classOf[Timestamp] -> TsCdc,
    classOf[java.util.Date] -> DateCdc,
    classOf[java.util.TimeZone] -> TzCdc,
    classOf[java.util.Locale] -> LocCdc,
    classOf[Number] -> BDCdc,
    classOf[EmailAddress] -> EmlCdc,
    classOf[Password] -> PwdCdc,
    classOf[GeoPoint] -> GeoPointCdc,
    classOf[java.net.URL] -> UrlCdc,
    classOf[java.net.URI] -> UriCdc)

  private def convertProxyValue(name: String, value: BsonField, asType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]]) =
    mapping.get(asType) match {
      case Some(converter) =>
        value match {
          case value: Value => value.as(converter)
          case _ => null
        }
      case None =>
        import scuff._
        value match {
          case value: Value =>
            if (asType.isInstance(value.raw)) {
              value.raw
            } else if (isProxyable(asType)) {
              getProxy(value.as[DBObject], mapping)(ClassTag[Any](asType))
            } else {
              scuff.reflect.DynamicConstructor[Any](value.raw)(ClassTag(asType)) getOrElse {
                throw new InvalidValueTypeException(name, s"Cannot convert ${value.raw.getClass.getName}(${value.raw}) to ${asType.getName}")
              }
            }
          case _ => null
        }
    }
  private def convertProxySet(name: String, shouldBeSet: BsonField, setType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]]): Set[_] = mapping.get(setType) match {
    case Some(converter) => shouldBeSet.asSeq(converter).toSet
    case None =>
      if (setType.isInterface) {
        shouldBeSet.asSeq[DBObject].map(getProxy(_, mapping)(ClassTag[Any](setType))).toSet
      } else {
        shouldBeSet match {
          case value: Value => value.raw match {
            case list: java.util.ArrayList[_] =>
              import collection.JavaConverters._
              list.asScala.map(elem => convertProxyValue(name, BsonField(elem), setType, mapping)).toSet
            case _ => Set(convertProxyValue(name, value, setType, mapping))
          }
          case _ => Set.empty
        }
      }
  }
  private def convertProxyMap(name: String, shouldBeMap: BsonField, valType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]]): Map[String, _] = {
    shouldBeMap match {
      case value: Value =>
        mapping.get(valType) match {
          case Some(valConv) => MapCdc(valConv).decode(value)
          case None => value.raw match {
            case dbo: DBObject =>
              var map = Map.empty[String, Any]
              dbo.keys.foreach { key =>
                val value = convertProxyValue(key, dbo(key), valType, mapping)
                map += key -> value
              }
              map
            case e: Exception => throw new InvalidValueTypeException(name)(e, s"Cannot convert ${value.raw.getClass.getName}(${value.raw}) to Map[String, ${valType.getName}]")
          }
        }
      case _ => Map.empty
    }
  }

  private def convertProxySeq(name: String, shouldBeList: BsonField, seqType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]]): IndexedSeq[_] = mapping.get(seqType) match {
    case Some(converter) => shouldBeList.asSeq(converter)
    case None =>
      if (seqType.isInterface) {
        shouldBeList.asSeq[DBObject].map(getProxy(_, mapping)(ClassTag[Any](seqType)))
      } else {
        shouldBeList match {
          case value: Value => value.raw match {
            case list: java.util.List[_] =>
              import collection.JavaConversions._
              list.toArray().map(elem => convertProxyValue(name, BsonField(elem), seqType, mapping))
            case _ => IndexedSeq(convertProxyValue(name, value, seqType, mapping))
          }
          case _ => IndexedSeq.empty
        }
      }
  }
  private def convertProxyList(name: String, shouldBeList: BsonField, seqType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]]): List[_] = mapping.get(seqType) match {
    case Some(converter) => shouldBeList.asList(converter)
    case None =>
      if (seqType.isInterface) {
        shouldBeList.asList[DBObject].map(getProxy(_, mapping)(ClassTag[Any](seqType)))
      } else {
        shouldBeList match {
          case value: Value => value.raw match {
            case list: java.util.ArrayList[_] =>
              import collection.JavaConverters._
              list.asScala.map(elem => convertProxyValue(name, BsonField(elem), seqType, mapping)).toList
            case _ => List(convertProxyValue(name, value, seqType, mapping))
          }
          case _ => Nil
        }
      }
  }

  private def getGenericReturnClass(method: java.lang.reflect.Method, depth: Int) = {
      def getGenericType(t: java.lang.reflect.Type, depth: Int): Class[_] = {
        val typeArgs = t match {
          case pt: java.lang.reflect.ParameterizedType => pt.getActualTypeArguments()
        }
        typeArgs(typeArgs.length - 1) match {
          case pt: java.lang.reflect.ParameterizedType =>
            if (depth == 0) {
              pt.getRawType.asInstanceOf[Class[_]]
            } else {
              getGenericType(pt, depth - 1)
            }
          case clz: Class[_] => clz
        }
      }
    getGenericType(method.getGenericReturnType, depth)
  }

  private def extractValue(valueName: String, method: java.lang.reflect.Method, value: BsonField,
    expectedType: Class[_], mapping: Map[Class[_], Codec[_, BsonValue]], depth: Int = 0): Any = try {
    if (expectedType == classOf[Object]) {
      convertProxyValue(valueName, value, expectedType, mapping)
    } else if (expectedType == classOf[Option[_]]) {
      val rtt = getGenericReturnClass(method, depth)
      Option(extractValue(valueName, method, value, rtt, mapping, depth + 1))
    } else if (expectedType.isAssignableFrom(classOf[IndexedSeq[_]])) {
      val rtt = getGenericReturnClass(method, depth)
      convertProxySeq(valueName, value, rtt, mapping)
    } else if (expectedType == classOf[List[_]]) {
      val rtt = getGenericReturnClass(method, depth)
      convertProxyList(valueName, value, rtt, mapping)
    } else if (expectedType.isAssignableFrom(classOf[Set[_]])) {
      val rtt = getGenericReturnClass(method, depth)
      convertProxySet(valueName, value, rtt, mapping)
    } else if (expectedType.isAssignableFrom(classOf[Map[_, _]])) {
      val rtt = getGenericReturnClass(method, depth)
      convertProxyMap(valueName, value, rtt, mapping)
    } else {
      convertProxyValue(valueName, value, expectedType, mapping)
    }
  } catch {
    case e: UnavailableValueException => throw e
    case e: Exception => throw new InvalidValueTypeException(valueName, e)
  }

  def getProxy[T: ClassTag](dbo: BsonObject, userMapping: Map[Class[_], Codec[_, BsonValue]]): T = {
      def checkNotNull(any: Any, name: String): Any = any match {
        case null => throw new UnavailableValueException(name)(s"$name is null")
        case _ => any
      }
    val fp = new Proxylicious[T]
    val mapping = DefaultProxyMapping ++ userMapping
    val proxy = fp.proxify {
      case (_, method, args) if args == null || args.length == 0 =>
        if (method.getName == "toString") {
          dbo.toJson()
        } else {
          checkNotNull(extractValue(method.getName, method, dbo(method.getName), method.getReturnType, mapping), method.getName)
        }
      case (_, method, args) if args != null && args.length == 1 && args(0).isInstanceOf[String] =>
        val valueName = args(0).asInstanceOf[String]
        checkNotNull(extractValue(valueName, method, dbo(valueName), method.getReturnType, mapping), valueName)
      case (_, method, _) => throw new IllegalAccessException("Cannot proxy methods with arguments: " + method)
    }
    fp.withEqualsHashCodeOverride(proxy)
  }

  private def isProxyable(cls: Class[_]): Boolean = cls.isInterface && cls.getMethods.forall { m =>
    m.getParameterTypes.length match {
      case 0 => true
      case 1 => m.getParameterTypes()(0) == classOf[String]
      case _ => false
    }
  }

}
