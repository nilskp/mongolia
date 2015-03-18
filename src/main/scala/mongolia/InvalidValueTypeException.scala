package mongolia

case class InvalidValueTypeException(fieldName: String)(cause: Throwable, message: String)
    extends RuntimeException(message, cause) {
  def this(name: String, cause: Throwable) = this(name)(cause, cause.getMessage)
  def this(name: String, message: String) = this(name)(null, message)
}
