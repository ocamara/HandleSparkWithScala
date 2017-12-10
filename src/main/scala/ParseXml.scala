import scala.collection.mutable
import scala.xml.XML

/**
  * Created by ousminho on 10/12/17.
  */
object ParseXml {

  def example()=
  {
    val value = "<personne><nom>Camara</nom><prenom>Ousmane</prenom><ecole>Koffi,dauphine</ecole></personne>"
    val res=parse(value)
    res.foreach(println)
  }

  def parse(value:String):List[String]=
  {
    val data =XML.loadString(value)
    val childs=data.child
    val res=mutable.MutableList[String]()
    for(child<-childs)
      {
        res += child.text
      }
    return res.toList
  }

}
