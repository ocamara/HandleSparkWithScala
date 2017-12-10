import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import play.api.libs.json._

import scala.collection.mutable

/**
  * Created by ousminho on 09/12/17.
  */
object ParseFile {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[4]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val sc=spark.sparkContext
  case class Personne(firstname:String, lastname:String, date:Date, telephone:String, salary:Double, state:String, option:String, level:String)
  case class Poste(Code_commune_INSEE:String, Nom_commune:String, Code_postal:Int, Libelle_acheminement:String, Ligne_5:String, longitude:Double, latitude:Double)

  def parseTest()=
  {
    val file="/home/ousminho/preparation"
    val content=sc.textFile(file)
    val header=content.first()
    val parts=content.filter(line => ! line.equals(header)).map(line=>line.toString.split(";"))
    val initialRows=parts.map(line =>(line,parseJson(line(5))))
    val rows=initialRows.map(tuple =>
     {
        val oldFields = tuple._1
        val newFields = tuple._2
        val firsname = oldFields(0)
        val lastname = oldFields(1)
        val date = convertToDate(oldFields(2))
        val telephone = deleteWhiteSpaces(oldFields(3))
        val salary = convertToDouble(oldFields(4))
        val state = newFields(0)
        val option = newFields(1)
        val level = newFields(2)
        Personne(firsname,lastname,date,telephone,salary,state,option,level)
     })
    val df=spark.createDataFrame(rows)
    df.show()

  }

  def affect(line:Array[String],position:Int): String =
  {
    var res=""
    if(line.length>position)
      res=line(position)
    return res
  }

  def parse(): Unit =
  {
    val file="/home/ousminho/Téléchargements/laposte_hexasmal.csv"
    val content=sc.textFile(file)
    val header=content.first()
    val parts=content.filter(line => ! line.equals(header))
                      .map(line=>line.toString.split(";"))
    println("avant map partition")
   //val data= parts.mapPartitionsWithIndex(applyPartitionWithIndex)
   // data.foreach(println)
   println("total ",content.count())

    val data=parts.map (line=>
         {
           val codeCommune=affect(line,0)
           val commune=affect(line,1)
           val codePostal=convertToInt(affect(line,2))
           val libelle=affect(line,3)
           var ligne5=affect(line,4)
           var gps=Array("")
           if(line.length>5)
               gps=line(5).split(",")
           val longitude=convertToDouble(affect(gps,0))
           val latitude=convertToDouble(affect(gps,1))
           Poste(codeCommune,commune,codePostal,libelle,ligne5,longitude,latitude)
         })
    val df=spark.createDataFrame(data)
    df.printSchema()
    //df.show()
    df.createOrReplaceTempView("poste")
    val res=spark.sql("select * from poste p1 left outer join poste p2 on p1.code_commune_insee=p2.code_commune_insee where p1.Code_postal  between 20999 and 21005 and p1.nom_commune='DIJON'")
    res.show()
  }

  def parseJson(informations:String):Array[String]=
  {

    val extract = Json.parse(informations).toString().replace("\"","")
    val res=extract.substring(1,extract.length-1)
    val parts=res.split(",")
    var fields=""
    for(part<-parts)
      {
        fields += part.split(":")(1)+"-"
      }
    return fields.substring(0,fields.length-1).split("-")
  }

  def convertToDate(date:String):Date=
  {
    val formatDate=new SimpleDateFormat("dd-MM-yyyy")
    val time=formatDate.parse(date).getTime
    return new Date(time)
  }

  def deleteWhiteSpaces(string:String):String=
  {
    val clean=string.replace(" ","")
    return clean
  }

  def convertToDouble(value:String):Double=
  {
    if(value.isEmpty)
      return 0.0
    return deleteWhiteSpaces(value).toDouble
  }

  def convertToInt(value:String):Int=
  {
    if(value.isEmpty)
      return 0
    return deleteWhiteSpaces(value).toInt
  }

  def applyPartition[String](iter:Iterator[String]):Iterator[Poste]=
  {
    val liste=mutable.MutableList[Poste]()
    var total=0
    while(iter.hasNext) {
      total += 1
      val line=iter.next().toString.split(";")
      val codeCommune=affect(line,0)
      val commune=affect(line,1)
      val codePostal=convertToInt(affect(line,2))
      val libelle=affect(line,3)
      var ligne5=affect(line,4)
      var gps=Array("")
      if(line.length>5)
        gps=line(5).split(",")
      val longitude=convertToDouble(affect(gps,0))
      val latitude=convertToDouble(affect(gps,1))
      liste += new Poste(codeCommune,commune,codePostal,libelle,ligne5,longitude,latitude)
    }
    println("nouvelle partitition "+total)
    return liste.iterator
  }

  def applyPartitionWithIndex[String](index:Int,iter:Iterator[String]):Iterator[Poste]=
  {
    val liste=mutable.MutableList[Poste]()
    var total=0
    while(iter.hasNext) {
      total += 1
      val line=iter.next().toString.split(";")
      val codeCommune=affect(line,0)
      val commune=affect(line,1)
      val codePostal=convertToInt(affect(line,2))
      val libelle=affect(line,3)
      var ligne5=affect(line,4)
      var gps=Array("")
      if(line.length>5)
        gps=line(5).split(",")
      val longitude=convertToDouble(affect(gps,0))
      val latitude=convertToDouble(affect(gps,1))
      liste += new Poste(codeCommune,commune,codePostal,libelle,ligne5,longitude,latitude)
    }
    println("nouvelle partitition "+index+" total donnne "+total)
    return liste.iterator
  }

  def exampleWithCombiner(): Unit =
  {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc=spark.sparkContext
    val file_customers=sc.textFile("/home/ousminho/Téléchargements/data1")
    val file_orders=sc.textFile("/home/ousminho/Téléchargements/data2")
    val header_customers=file_customers.first()
    val header_orders=file_orders.first()
    val data_customers=file_customers.filter(line => ! line.equals(header_customers))
      .map(line=>line.split(","))
      .map(parts =>(parts(0),parts(1)))

    val data_orders=file_orders.filter(line => ! line.equals(header_orders))
      .map(line=>line.split(","))
      .map(parts =>(parts(2),parts(3).toInt))

      val data_orders_combine=data_orders.combineByKey(
          (amount:Int) => {
            println(s"Create combiner -> ${amount}")
                   (amount, 1)
                 },
               (acc: (Int, Int), v:Int) => {
                 println(s"""Merge value : (${acc._1} + ${v}, ${acc._2} + 1)""")
                 (acc._1 + v, acc._2 + 1)
               },
               (acc1: (Int, Int), acc2: (Int, Int)) => {
                 println(s"""Merge Combiner : (${acc1._1} + ${acc2._1}, ${acc1._2} + ${acc2._2})""")
                 (acc1._1 + acc2._1, acc1._2 + acc2._2)
               }
      ).mapValues(
          line=>(line._1.toInt/line._2.toInt)
        )
    //data_orders.foreach(println)
    //data_customers.zip(data_orders).foreach(println)
    //data_customers.join(data_orders).foreach(println)
    //data_customers.cogroup(data_orders).foreach(println)
     data_orders.aggregateByKey((0, 0))(
      (acc:(Int,Int), value:Int) => (acc._1 + value, acc._2 + 1),
      (acc1:(Int,Int), acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).foreach(println)
  }

}
