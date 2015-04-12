[![Build Status](https://drone.io/github.com/nilskp/mongolia/status.png)](https://drone.io/github.com/nilskp/mongolia/latest)
[![Scala version](https://img.shields.io/badge/scala-2.11-orange.svg)](http://www.scala-lang.org/api/2.11.6/)
[![Download](https://api.bintray.com/packages/nilskp/maven/Mongolia/images/download.svg) ](https://bintray.com/nilskp/maven/Mongolia/_latestVersion#files)
[![Join the chat at https://gitter.im/nilskp/mongolia](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/nilskp/mongolia?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Mongolia
========
Type safe Scala DSL for MongoDB, that has 3 simple goals:

1. Type-safety. Prevent runtime exceptions by allowing only BSON types or types that can be converted to BSON.
2. Fix the Java driver's UUID bug in a backwards compatible manner.
3. Keep syntax close to original Javascript 

Basic example:
```scala
import mongolia._
import BsonCodecs._

val personId = new ObjectId

val person = obj(
  _id(personId), // equivalent to "_id" := personId
  "name" := obj(
    "first" := "Francis",
    "middle" := "Joseph",
    "last" := "Underwood"
  ),
  "titles" := arr("Majority Whip", "Vice-president", "President")
)

coll.insert(person)

coll.findOpt(_id(personId)) match {
  case None => sys.error(s"Not found: $personId")
  case Some(person) =>
    val firstName: String = person("name.first").as[String]
    val middleName: Option[String] = person("name.middle").opt[String]
    val lastName: String = person("name.last").as[String]
    val titles: List[String] = person("titles").asList[String]
```
