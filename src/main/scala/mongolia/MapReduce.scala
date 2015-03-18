package mongolia

import scuff.js.CoffeeScriptCompiler

case class MapReduce(mapJS: String, reduceJS: String) {
  require(mapJS.trim.length > 0, "No JS code for `map` function")
  require(reduceJS.trim.length > 0, "No JS code for `reduce` function")
}
object MapReduce {
  private val FunctionMatcher = """^(map|reduce)\s*=\s*""".r.pattern
  private def compileCoffeeFunction(func: String, compiler: CoffeeScriptCompiler) = {
    val js = compiler.compile(func).trim()
    js.substring(1, js.length - 2)
  }
  def coffee(map: String, reduce: String)(implicit compiler: CoffeeScriptCompiler): MapReduce = {
    val mapCoffee = compileCoffeeFunction(map, compiler)
    val reduceCoffee = compileCoffeeFunction(reduce, compiler)
    new MapReduce(mapCoffee, reduceCoffee)
  }

  /**
   * Takes an `InputStream` which is expected to have
   * 2 function declarations named `map` and `reduce`,
   * in CoffeeScript.
   */
  def brew(coffeescript: java.io.InputStream, encoding: String = "UTF-8")(implicit compiler: CoffeeScriptCompiler): MapReduce =
    brew(new java.io.InputStreamReader(coffeescript, encoding))

  /**
   * Takes a `Reader` which is expected to have
   * 2 function declarations named `map` and `reduce`,
   * in CoffeeScript.
   */
  def brew(coffeescript: java.io.Reader)(implicit compiler: CoffeeScriptCompiler): MapReduce = {
    var active: Option[StringBuilder] = None
    val map = new StringBuilder
    val reduce = new StringBuilder
    val br = coffeescript match {
      case br: java.io.BufferedReader => br
      case r => new java.io.BufferedReader(r)
    }
    var line = br.readLine()
    while (line != null) {
      val m = FunctionMatcher.matcher(line)
      if (m.lookingAt()) {
        m.group(1) match {
          case "map" => active = Some(map)
          case "reduce" => active = Some(reduce)
        }
        line = m.replaceFirst("")
      }
      active.foreach(_ append line append '\n')
      line = br.readLine()
    }
    require(map.size > 0, "Map function not found. Must be named `map`.")
    require(reduce.size > 0, "Reduce function not found. Must be named `reduce`.")
    coffee(map.result, reduce.result)
  }
}
