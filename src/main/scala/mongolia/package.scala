import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.util.UUID
import scala.collection.GenTraversableOnce
import scala.language.implicitConversions
import scala.reflect.ClassTag
import org.bson.types._
import com.mongodb._
import scala.util.Try
import javax.script.ScriptEngine
import scuff._
import scuff.js.CoffeeScriptCompiler

/**
 * Convenience DSL for the MongoDB Java driver.
 */
package object mongolia {

  trait BsonValue { def raw: Any }

  private[mongolia] def throwCoercionException(from: Any, to: String) =
    throw new RuntimeException(s"Cannot coerce ${from.getClass.getName}($from) into $to")

  import BsonCodecs._

  implicit def toAssignment(key: String) = new Assignment(key)
  implicit def toAssignment(key: scala.Symbol) = new Assignment(key.name)

  private[mongolia] def geo2Dbo(gp: GeoPoint): BsonObject = obj("type" := "Point", "coordinates" := arr(gp.longitude: Double, gp.latitude: Double))

  private[mongolia] def any2Array[T](any: Any)(implicit codec: Codec[T, BsonValue], tag: ClassTag[T]): Array[T] = {
    val list: Iterable[_] = anyToIterable(any)
    val array = new Array[T](list.size)
    var i = 0
    val iter = list.iterator
    while (i < array.length) {
      iter.next match {
        case null => // Ignore
        case a => array(i) = codec.decode(new Value(a))
      }
      i += 1
    }
    array
  }

  private[mongolia] def seq2bsonlist[T](seq: GenTraversableOnce[T])(implicit codec: Codec[T, BsonValue]) = {
    val list = new BsonList
    seq.foreach { t =>
      val value = if (t == null) null else {
        val bson = codec.encode(t)
        if (bson == null) null else bson.raw
      }
      list += value
    }
    list
  }

  implicit def id2obj(oid: ObjectId): DBObject = obj("_id" := oid)

  def obj(ignoreNulls: Boolean = false, ignoreEmpty: Boolean = false): BsonObject = new BsonObject(ignoreNulls = ignoreNulls, ignoreEmpty = ignoreEmpty)
  def obj(props: Seq[BsonProp]): BsonObject = {
    val map = new BsonObject
    props.foreach(p => if (map.put(p.key, p.raw) != null) throw new IllegalArgumentException("Field \"%s\" occurs multiple times".format(p.key)))
    map
  }
  def obj(head: BsonProp, tail: BsonProp*): BsonObject = {
    val map = new BsonObject(new BasicDBObject(head.key, head.raw))
    tail.foreach(p => if (map.put(p.key, p.raw) != null) throw new IllegalArgumentException("Field \"%s\" occurs multiple times".format(p.key)))
    map
  }
  def arr(values: BsonValue*): BsonList = seq2bsonlist(values)(Codec.noop)
  def arr[T](values: T*)(implicit codec: Codec[T, BsonValue]): BsonList = seq2bsonlist(values)
  def obj[T](map: collection.Map[String, T])(implicit codec: Codec[T, BsonValue]): BsonObject = new BsonObject().add(map)

  implicit def enrich(poor: DBObject) = poor match {
    case rich: BsonObject => rich
    case _ => new BsonObject(poor)
  }
  implicit def impoverish(rich: BsonObject) = rich.impoverish
  implicit def impoverish(rich: DocCollection) = rich.impoverish
  implicit def impoverish(rich: BsonCursor) = rich.impoverish
  implicit def impoverish(rich: Database) = rich.impoverish

  val ASC, LAST, INCLUDE = 1
  val DESC, FIRST = -1
  val EXCLUDE = 0

  private[mongolia] def binaryType4ToUUID(array: Array[Byte]): UUID = {
    val bb = java.nio.ByteBuffer.wrap(array)
    new UUID(bb.getLong, bb.getLong)
  }

  private[mongolia] def anyToIterable(any: Any): Iterable[_] = {
    import collection.JavaConversions._
    any match {
      case arr: Array[AnyRef] => arr
      case list: java.lang.Iterable[_] => list
      case seq: collection.GenTraversableOnce[_] => seq.toIterable.asInstanceOf[Iterable[_]]
      case _ => throwCoercionException(any, "java.util.List")
    }
  }

  implicit final class Database(val underlying: DB) extends AnyVal {
    def impoverish = underlying
    def apply(collection: String, wc: WriteConcern = null) = {
      val dbColl = underlying.getCollection(collection)
      if (wc != null) dbColl.setWriteConcern(wc)
      new DocCollection(dbColl)
    }
    def enrich: Database = this
  }

  implicit final class DocCollection(val underlying: DBCollection) extends AnyVal {
    implicit def impoverish = underlying
    private def SAFE = underlying.getWriteConcern.getWObject.asInstanceOf[Any] match {
      case w: Int if w < WriteConcern.SAFE.getW => WriteConcern.SAFE
      case _ => underlying.getWriteConcern
    }
    def safeInsert(dbo: DBObject, more: DBObject*) = {
      if (more.isEmpty) {
        underlying.insert(dbo, SAFE)
      } else {
        val all = Array(dbo) ++ more
        underlying.insert(all, SAFE)
      }
    }
    def safeInsert(dbos: Iterable[DBObject]) = underlying.insert(dbos.toArray, SAFE)
    def safeSave(dbo: DBObject) = underlying.save(dbo, SAFE)
    def safeUpdate(key: DBObject, upd: DBObject, upsert: Boolean = false, multi: Boolean = false) = underlying.update(key, upd, upsert, multi, SAFE)
    def safeUpdateMulti(key: DBObject, upd: DBObject) = underlying.update(key, upd, false, true, SAFE)
    def safeUpdateAtomic(key: DBObject, upd: DBObject) = {
      key.put("$atomic", true)
      safeUpdateMulti(key, upd)
    }
    def updateAtomic(key: DBObject, upd: DBObject) = {
      key.put("$atomic", true)
      underlying.updateMulti(key, upd)
    }
    def upsert(key: DBObject, upd: DBObject, concern: WriteConcern = underlying.getWriteConcern) = underlying.update(key, upd, true, false, concern)
    def safeUpsert(key: DBObject, upd: DBObject) = underlying.update(key, upd, true, false, SAFE)
    def safeRemove(key: DBObject) = underlying.remove(key, SAFE)
    def safeRemoveAtomic(key: DBObject) = {
      key.put("$atomic", true)
      underlying.remove(key, SAFE)
    }
    private def includeFields(fields: Seq[String]): DBObject = if (fields.isEmpty) null else obj(fields.map(_ := INCLUDE))
    def updateAndReturn(query: DBObject, upd: DBObject, returnFields: String*): Option[DBObject] = Option(underlying.findAndModify(query, includeFields(returnFields), null, false, upd, true, false))
    def upsertAndReturn(query: DBObject, upd: DBObject, returnFields: String*): DBObject = underlying.findAndModify(query, includeFields(returnFields), null, false, upd, true, true)
    def unique(field: String, query: DBObject = null): Seq[BsonField] = {
      import collection.JavaConverters._
      val list = underlying.distinct(field, query).asInstanceOf[java.util.List[Any]]
      list.asScala.view.map(BsonField(_, null, field))
    }

    def mapReduceInline(mapReduce: MapReduce, query: DBObject = null): Iterator[DBObject] = {
      val javaIter = underlying.mapReduce(mapReduce.mapJS, mapReduce.reduceJS, null, MapReduceCommand.OutputType.INLINE, query).results.iterator
      new Iterator[DBObject] {
        def hasNext = javaIter.hasNext
        def next = javaIter.next
      }
    }

    private def mapReduceInto(mapReduce: MapReduce, coll: DBCollection, query: DBObject, outType: MapReduceCommand.OutputType) = {
      underlying.mapReduce(mapReduce.mapJS, mapReduce.reduceJS, coll.getFullName, outType, query)
    }
    def mapReduceMerge(mapReduce: MapReduce, mergeThis: DBCollection, query: DBObject = null): MapReduceOutput = {
      mapReduceInto(mapReduce, mergeThis, query, MapReduceCommand.OutputType.MERGE)
    }
    def mapReduceReduce(mapReduce: MapReduce, reduceThis: DBCollection, query: DBObject = null): MapReduceOutput = {
      mapReduceInto(mapReduce, reduceThis, query, MapReduceCommand.OutputType.REDUCE)
    }
    def mapReduceReplace(mapReduce: MapReduce, replaceThis: DBCollection, query: DBObject = null): MapReduceOutput = {
      mapReduceInto(mapReduce, replaceThis, query, MapReduceCommand.OutputType.REPLACE)
    }

    def createIndex(key: String): Unit = underlying.createIndex(obj(key := ASC))
    def createIndex(key: String, idxType: String): Unit = underlying.createIndex(obj(key := idxType))
    def createIndex(keyHead: BsonIntProp, keyTail: BsonIntProp*): Unit = underlying.createIndex(obj(keyHead, keyTail: _*))
    def createUniqueIndex(key: String): Unit = underlying.createIndex(obj(key := ASC), obj("unique" := true))
    def createHashedIndex(key: String): Unit = underlying.createIndex(obj(key := "hashed"))
    def createTextIndex(fields: Set[String], langField: String = null, defaultLang: Locale = null, indexName: String = null): Unit = {
      require(fields.nonEmpty, "Must have at least one field to index on")
      val keys = fields.foldLeft(obj()) {
        case (keys, field) => keys(field := "text")
      }
      val defLang = Option(defaultLang).filterNot(_ == Locale.ROOT).map(_.toLanguageTag).getOrElse("none")
      val options = obj("default_language" := defLang)
      Option(langField).foreach { langField =>
        options("language_override" := langField)
      }
      Option(indexName).foreach(name => options("name" := name))
      underlying.createIndex(keys, options)
    }
    def createSparseIndex(key: String): Unit = underlying.createIndex(obj(key := ASC), obj("sparse" := true))
    def createUniqueSparseIndex(key: String): Unit = underlying.createIndex(obj(key := ASC), obj("sparse" := true, "unique" := true))
    def createUniqueIndex(keyHead: BsonIntProp, keyTail: BsonIntProp*): Unit = underlying.createIndex(obj(keyHead, keyTail: _*), obj("unique" := true))
    def findOne(anyRef: Object) = anyRef match {
      case key: DBObject => underlying.findOne(key)
      case prop: BsonProp => underlying.findOne(obj(prop))
      case value: BsonValue => underlying.findOne(_id(value))
      case _ => underlying.findOne(anyRef)
    }
    def findOpt(key: DBObject, fields: DBObject = null) = underlying.findOne(key, fields) match {
      case null => None
      case doc => Some(doc.enrich)
    }
  }

  final class BsonObject(private val underlying: DBObject = new BasicDBObject, ignoreNulls: Boolean = false, ignoreEmpty: Boolean = false) extends DBObject with BsonValue {
    import collection.JavaConverters._
    if (underlying == null) throw new NullPointerException("Document is null")
    def markAsPartialObject = underlying.markAsPartialObject
    def isPartialObject: Boolean = underlying.isPartialObject
    def put(key: String, v: Any) = v match {
      case null if ignoreNulls => underlying.removeField(key)
      case l: java.util.List[_] if ignoreEmpty && l.isEmpty => underlying.removeField(key)
      case _ => underlying.put(key, v)
    }
    def putAll(o: org.bson.BSONObject) = o match {
      case m: java.util.Map[_, _] => putAll(m: java.util.Map[_, _])
      case _ => for (k ← o.keySet.asScala) put(k, o.get(k))
    }
    def putAll(m: java.util.Map[_, _]) = for (entry ← m.entrySet.asScala) put(entry.getKey.toString, entry.getValue)
    def get(key: String) = underlying.get(key)
    def getOrUpdate[T](key: String, ctor: => T)(implicit codec: Codec[T, BsonValue]): T = {
      apply(key) match {
        case v: Value => codec.decode(v)
        case _ =>
          val t = ctor
          apply(key := t)
          t
      }
    }

    /**
     * Modify, mutable, object.
     * @param key The key name
     * @param initValue The value to initialize with, if key is missing (or null)
     * @param updater The updater function
     */
    def modify[T <: AnyRef](key: String, initValue: => T)(modifier: T => Unit)(implicit codec: Codec[T, BsonValue]): Unit = {
      val currT = apply(key) match {
        case v: Value => codec.decode(v)
        case _ =>
          val newT = initValue
          apply(key := newT)
          newT
      }
      modifier(currT)
    }

    /**
     * Update, immutable, value.
     * @param key The key name
     * @param initValue Optional. The value to initialize with, if key is missing or null.
     * If not provided, no update will happen if key is missing or null
     * @param updater The updater function
     */
    def update[T](key: String, initValue: T = null)(updater: T => T)(implicit codec: Codec[T, BsonValue]): Unit = {
      val currT = apply(key) match {
        case v: Value => codec.decode(v)
        case _ => initValue
      }
      if (currT != null) {
        apply(key := updater(currT))
      }
    }

    def replace(prop: BsonProp): BsonField = {
      val current = apply(prop.key)
      apply(prop)
      current
    }
    def toMap = underlying.toMap
    def removeField(key: String) = underlying.removeField(key)
    @deprecated(message = "Deprecated", since = "Don't care")
    def containsKey(s: String) = underlying.containsKey(s)
    def containsField(s: String) = underlying.containsField(s)
    def keySet = underlying.keySet
    def add[T](map: collection.Map[String, T])(implicit codec: Codec[T, BsonValue]): BsonObject = {
      map.foreach {
        case (key, value) => add(key := value)
      }
      this
    }
    def rename(fromTo: (String, String)): Unit = rename(fromTo, null)(null)
    def rename[T](fromTo: (String, String), transform: BsonField => T)(implicit codec: Codec[T, BsonValue]): Unit = {
      if (underlying.containsField(fromTo._1)) {
        val removed = underlying.removeField(fromTo._1)
        if (transform == null) {
          underlying.put(fromTo._2, removed)
        } else {
          val value = transform(BsonField(removed, null, fromTo._1))
          this.add(fromTo._2 := value)
        }
      }
    }
    def raw = this
    /**
     * Ignore `null` values, i.e. don't put them into doc.
     */
    def ignoreNulls(ignore: Boolean): BsonObject = if (ignore == this.ignoreNulls) this else new BsonObject(underlying, ignore, this.ignoreEmpty)
    /**
     * Ignore empty arrays, i.e. don't put them into doc.
     */
    def ignoreEmpty(ignore: Boolean): BsonObject = if (ignore == this.ignoreEmpty) this else new BsonObject(underlying, this.ignoreNulls, ignore)
    def keys: Iterable[String] = underlying.keySet.asScala
    /**
     * Much faster and more compact serialization,
     * and more importantly *correct* JSON.
     */
    def toJson(sb: java.lang.StringBuilder): java.lang.StringBuilder = Json.toJson(underlying, sb)
    def toJson(): String = Json.toJson(underlying).toString
    override def toString = underlying.toString()
    import java.util.StringTokenizer
    import collection.JavaConverters._

    @annotation.tailrec
    private[this] def getNested(obj: DBObject, nested: StringTokenizer, remove: Boolean): Any = {
      if (obj == null) {
        null
      } else {
        val name = nested.nextToken()
        if (nested.hasMoreTokens()) {
          getNested(obj.get(name).asInstanceOf[DBObject], nested, remove)
        } else if (remove) {
          obj.removeField(name)
        } else {
          obj.get(name)
        }
      }
    }
    @annotation.tailrec
    private[this] def containsNested(obj: DBObject, nested: StringTokenizer): Boolean = {
      if (obj == null) {
        false
      } else {
        val name = nested.nextToken()
        if (nested.hasMoreTokens()) {
          containsNested(obj.get(name).asInstanceOf[DBObject], nested)
        } else {
          obj.containsField(name)
        }
      }
    }

    def like[T](implicit tag: ClassTag[T], mapping: Map[Class[_], Codec[_, BsonValue]] = Map.empty): T =
      if (tag.runtimeClass.isInterface) {
        Proxying.getProxy(this, mapping)
      } else throw new IllegalArgumentException(s"${tag.runtimeClass} must be an interface")

    def isEmpty = underlying match {
      case m: java.util.Map[_, _] => m.isEmpty
      case _ => underlying.keySet.isEmpty
    }
    def prop(key: String): BsonProp = new BsonProp(key, new Value(underlying.get(key)))
    def asSeq[T](implicit codec: Codec[T, BsonValue]) = new Value(underlying).asSeq[T]
    def asSeqOfOption[T](implicit codec: Codec[T, BsonValue]) = new Value(underlying).asSeqOfOption[T]
    def add(head: BsonProp, tail: BsonProp*): BsonObject = {
      this.put(head.key, head.raw)
      tail.foreach { prop =>
        this.put(prop.key, prop.raw)
      }
      this
    }
    def add(props: Seq[BsonProp]): BsonObject = {
      props.foreach { prop =>
        this.put(prop.key, prop.raw)
      }
      this
    }
    def enrich = this
    def impoverish = underlying
    def _id = underlying.get("_id") match {
      case null => throw new IllegalArgumentException("Field \"_id\" is missing")
      case oid: ObjectId => oid
      case any =>
        import language.reflectiveCalls
        OIDCdc.decode(any)
    }
    def apply(key: String): BsonField = BsonField(getAs[Any](key), underlying, key)
    def apply(head: BsonProp, tail: BsonProp*): BsonObject = add(head, tail: _*)
    def has(key: String) = this.contains(key)
    def getAs[T](name: String): T = {
      if (name.indexOf('.') == -1) {
        underlying.get(name).asInstanceOf[T]
      } else {
        getNested(underlying, new java.util.StringTokenizer(name, "."), remove = false).asInstanceOf[T]
      }
    }

    def remove(key: String): BsonField = {
      val value = if (key.indexOf('.') == -1) {
        underlying.removeField(key)
      } else {
        getNested(underlying, new java.util.StringTokenizer(key, "."), remove = true)
      }
      BsonField(value, null, key)
    }

    def contains(name: String): Boolean = {
      if (name.indexOf('.') == -1) {
        underlying.containsField(name)
      } else {
        containsNested(underlying, new java.util.StringTokenizer(name, "."))
      }
    }

    def map[T](f: BsonObject => T): T = f(this)

  }

  final class BsonList extends BasicDBList with BsonValue {
    def raw = this
    /**
     * Much faster and more compact serialization,
     * and more importantly *correct* JSON, i.e.
     * `NaN` is translated to `null`.
     */
    def toJson(): String = Json.toJson(this).toString
    def +=(any: Any) = add(any.asInstanceOf[Object])
  }

  implicit final class BsonCursor(val cursor: DBCursor) extends AnyVal {
    def impoverish = cursor
    override def toString = map(_.toString).mkString("[", ",", "]")
    def toSeq() = map(d => d)
    def find(foundIt: BsonObject => Boolean): Option[BsonObject] = {
      var found: Option[BsonObject] = None
      try {
        while (found.isEmpty && cursor.hasNext) {
          val obj = new BsonObject(cursor.next)
          if (foundIt(obj)) {
            found = Some(obj)
          }
        }
      } finally {
        cursor.close()
      }
      found
    }
    def first(key: String): Option[BsonObject] = top(key := ASC)
    def last(key: String): Option[BsonObject] = top(key := DESC)
    def top(sorting: BsonIntProp, more: BsonIntProp*): Option[BsonObject] = try {
      val cursor = this.cursor.sort(obj(sorting, more: _*)).limit(1)
      if (cursor.hasNext) Some(cursor.next) else None
    } finally {
      cursor.close()
    }
    def foreach(f: BsonObject => Unit) {
      try {
        while (cursor.hasNext) {
          f(new BsonObject(cursor.next))
        }
      } finally {
        cursor.close()
      }
    }
    def getOrElse[T](f: BsonObject => T, t: => T): T = {
      try {
        if (cursor.hasNext) {
          f(new BsonObject(cursor.next))
        } else {
          t
        }
      } finally {
        cursor.close()
      }
    }
    def map[T](f: BsonObject => T): Seq[T] = {
      val buffer = collection.mutable.Buffer[T]()
      foreach { dbo =>
        buffer += f(dbo)
      }
      buffer
    }

    def flatMap[T](f: BsonObject => collection.GenTraversableOnce[T]): Seq[T] = {
      val buffer = collection.mutable.Buffer[T]()
      foreach { dbo =>
        f(dbo).foreach(buffer += _)
      }
      buffer
    }
    def foldLeft[T](initial: T)(f: (T, BsonObject) => T): T = {
      var t = initial
      foreach { dbo =>
        t = f(t, dbo)
      }
      t
    }
  }

  implicit def prop2obj(prop: BsonProp) = obj(prop)

  private def iter2List[T](i: java.lang.Iterable[T])(implicit codec: Codec[T, BsonValue]) = {
    val list = new BsonList
    var iter = i.iterator()
    while (iter.hasNext) {
      val t = codec.encode(iter.next)
      list += t.raw
    }
    list
  }

  def _id[T](value: T)(implicit codec: Codec[T, BsonValue]) = obj("_id" := value)
  private def escapeSearchTerm(term: String, exclude: Boolean): String = {
    val cleanTerm = if (term startsWith "-") term.substring(1) else term
    val escapedTerm =
      if (term.contains(' ')) {
        "\"" + term + "\""
      } else term
    if (exclude) {
      "-" concat term
    } else {
      term
    }
  }
  def $text(terms: Set[String], not: Set[String] = Set.empty, lang: Locale = null, limit: Option[Int] = None, filter: DBObject = null, project: DBObject = null) = {
    val escapedTerms = terms.iterator.map(escapeSearchTerm(_, exclude = false))
    val escapedNot = not.iterator.map(escapeSearchTerm(_, exclude = true))
    val allTerms = escapedTerms ++ escapedNot
    val textDoc = obj("$search" := allTerms.mkString(" "))
    val langTag = if (lang != null) lang.toLanguageTag else "none"
    textDoc("$language" := langTag)
    limit.foreach(limit => textDoc("limit" := limit))
    if (filter != null && !filter.isEmpty) {
      textDoc("filter" := filter)
    }
    if (project != null && !project.isEmpty) {
      textDoc("project" := project)
    }
    "$text" := textDoc
  }
  def $gt[T](value: T)(implicit codec: Codec[T, BsonValue]) = "$gt" := value
  def $gte[T](value: T)(implicit codec: Codec[T, BsonValue]) = "$gte" := value
  def $lt[T](value: T)(implicit codec: Codec[T, BsonValue]) = "$lt" := value
  def $ne[T](value: T)(implicit codec: Codec[T, BsonValue]) = "$ne" := value
  def $lte[T](value: T)(implicit codec: Codec[T, BsonValue]) = "$lte" := value
  def $size(size: Int) = "$size" := size
  def $type(bsonType: BsonType) = "$type" := bsonType.typeNumber
  def $all[T](values: T*)(implicit codec: Codec[T, BsonValue]) = "$all" := arr(values: _*)
  def $in[T](values: T*)(implicit codec: Codec[T, BsonValue]) = "$in" := arr(values: _*)
  def $nin[T](values: T*)(implicit codec: Codec[T, BsonValue]) = "$nin" := arr(values: _*)
  def $and[T](exprs: T*)(implicit codec: Codec[T, BsonValue]) = "$and" := arr(exprs: _*)
  def $or[T](exprs: T*)(implicit codec: Codec[T, BsonValue]) = "$or" := arr(exprs: _*)
  def $nor[T](exprs: T*)(implicit codec: Codec[T, BsonValue]) = "$nor" := arr(exprs: _*)
  def $each[T](values: Seq[T])(implicit codec: Codec[T, BsonValue]) = "$each" := arr(values: _*)
  def $each[T](value: T, more: T*)(implicit codec: Codec[T, BsonValue]) = "$each" := arr((value :: more.toList): _*)
  def $exists(exists: Boolean): BsonProp = "$exists" := exists
  def $set(props: Seq[BsonProp]): BsonProp = "$set" := obj(props)
  def $set(prop: BsonProp, more: BsonProp*): BsonProp = "$set" := obj(prop, more: _*)
  def $setOnInsert(props: Seq[BsonProp]): BsonProp = "$setOnInsert" := obj(props)
  def $setOnInsert(prop: BsonProp, more: BsonProp*): BsonProp = "$setOnInsert" := obj(prop, more: _*)
  def $unset(names: String*) = {
    val unsets = new BsonObject
    names.foreach { name =>
      unsets.add(name := 1)
    }
    "$unset" := unsets
  }
  def $mod(modBy: Int, equalsTo: Int) = "$mod" := arr(IntCdc.encode(modBy), IntCdc.encode(equalsTo))
  def $not(props: Seq[BsonProp]) = "$not" := obj(props)
  def $not(prop: BsonProp, more: BsonProp*) = "$not" := obj(prop, more: _*)
  def $inc(props: Seq[BsonNumProp]) = "$inc" := obj(props)
  def $inc(prop: BsonNumProp, more: BsonNumProp*) = "$inc" := obj(prop, more: _*)
  def $push(props: Seq[BsonProp]) = "$push" := obj(props)
  def $push(prop: BsonProp, more: BsonProp*) = "$push" := obj(prop, more: _*)
  def $addToSet(props: Seq[BsonProp]) = "$addToSet" := obj(props)
  def $addToSet(prop: BsonProp, more: BsonProp*) = "$addToSet" := obj(prop, more: _*)
  def $pushAll(prop: BsonProp) = "$pushAll" := obj(prop)
  def $pop(prop: BsonIntProp): BsonProp = "$pop" := obj(prop)
  def $pop(name: String): BsonProp = "$pop" := obj(name := LAST)
  def $pull(prop: BsonProp) = "$pull" := obj(prop)
  def $elemMatch(props: Seq[BsonProp]) = "$elemMatch" := obj(props)
  def $elemMatch(prop: BsonProp, more: BsonProp*) = "$elemMatch" := obj(prop, more: _*)
  def $near(point: GeoPoint): BsonProp = {
    val geoDbo = geo2Dbo(point)
    val near = obj("$geometry" := geoDbo)
    if (point.radius > 0f) near("$maxDistance" := point.radius)
    "$near" := near
  }
  def $regex(regex: String, options: String = "") = {
    val dbo = obj("$regex" := regex)
    if (options.length != 0) dbo("$options" := options)
    dbo
  }
  def $where(jsExpr: String) = "$where" := jsExpr
  def $currentDate(field: String, otherFields: String*): BsonProp = {
    val cdDoc = obj(field := true)
    if (otherFields.nonEmpty) {
      cdDoc.add(otherFields.map(_ := true))
    }
    "$currentDate" := cdDoc
  }
  def $currentDate(head: BsonProp, tail: BsonProp*): BsonProp = "$currentDate" := obj(head, tail: _*)

}
