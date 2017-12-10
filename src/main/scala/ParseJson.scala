import scala.util.parsing.json.JSON

/**
  * Created by ousminho on 10/12/17.
  */
object ParseJson {

  def example()=
  {
    val value="""{"nom":"camara","prenom":"ousmane","ecole":["koffi","dauphine"]}"""
    val result=parse(value)
    result.foreach(println)

  }

  def parse(value:String):List[String]=
  {
    val result=JSON.parseFull(value)
    result match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, String]) =>  return map.values.toList
      case None => return List()
      case other => return List()
    }

  }
}
