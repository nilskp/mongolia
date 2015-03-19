package mongolia

case class UnavailableValueException(fieldName: String)(message: String)
  extends RuntimeException(message)
