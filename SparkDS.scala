// Databricks notebook source
import sys.process._
import scala.util.Sorting
import scala.collection.mutable.WrappedArray 
"wget -P /tmp https://www.datacrucis.com/media/datasets/stratahadoop-BCN-2014.json" !!
val localpath="file:/tmp/stratahadoop-BCN-2014.json"
dbutils.fs.mkdirs("dbfs:/datasets/")
dbutils.fs.cp(localpath, "dbfs:/datasets/")
display(dbutils.fs.ls("dbfs:/datasets/stratahadoop-BCN-2014.json"))
val df = sqlContext.read.json("dbfs:/datasets/stratahadoop-BCN-2014.json")

// COMMAND ----------

case class Nested(indices:Array[Long],text: String)
case class Entities(hashtags:Seq[Nested])
case class TestClass (created_at:String,entities: Entities,user:User)
case class User(name:String)
val ds = df.as[TestClass].repartition(8)

// COMMAND ----------

val rdd = ds.rdd.flatMap(i=>i.entities.hashtags)
val wordCounts = rdd.map(word => (word.text,1)).reduceByKey(_+_)
//top()(Ordering.by(e=>e._2)) means we top this array according to the value, default is key.
val sortedArray = wordCounts.top(wordCounts.count.toInt)(Ordering.by(e => e._2))
sortedArray.foreach(i=>println(i))

// COMMAND ----------

val rddReduceSorted = ds.rdd.map(i=>(i.user.name,1)).reduceByKey(_+_).top(ds.count.toInt)(Ordering.by(e => e._2))
rddReduceSorted.foreach(i=>println(i))

// COMMAND ----------

val rddAndReduce = ds.rdd
.flatMap(i=>{
  val b = i.created_at.split(" ")
  val day = b(2).toInt
  val mon = b(1) match{
    case "Jan" => 1
    case "Feb" => 2
    case "Mar" => 3
    case "Apr" => 4
    case "May" => 5
    case "Jun" => 6
    case "Jul" => 7
    case "Aug" => 8
    case "Sep" => 9
    case "Oct" => 10
    case "Nov" => 11
    case "Dec" => 12
  }
  val year = b(5).toInt
  i.entities.hashtags.map(p=>((day,mon,year,p.text),1))
  })
.reduceByKey(_+_)

// COMMAND ----------

def findByDay(day:Int,month:Int,year:Int)={
  rddAndReduce.map(i=>{
  ((i._1._1,i._1._2,i._1._3),(i._1._4,i._2))
})
.filter(i=>i._1._3==year&&i._1._2==month&&i._1._1==day)
}
def findByAllDay()={
  rddAndReduce.map(i=>{
  ((i._1._1,i._1._2,i._1._3),(i._1._4,i._2))
  //Same as sortByKey()
})
.sortBy(_._2)
.sortBy(_._1)
}
def findByTag(tag:String)={
  rddAndReduce.map(i=>{
   (i._1._4,(i._1._1,i._1._2,i._1._3),i._2)
  //Same as sortByKey()
  })
  .filter(i=>i._1==tag)
}

//ex:
findByDay(18,11,2014)
.take(10)
.foreach(println)
println
findByTag("BigData")
.top(20)(Ordering.by(e=>e._2._1))
.foreach(println)
println
findByAllDay()
.take(rddAndReduce.count.toInt)
.foreach(println)

