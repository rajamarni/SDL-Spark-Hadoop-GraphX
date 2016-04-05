package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import java.io._
import scalax.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark._
import scala.collection.mutable.HashMap
import org.apache.spark.{AccumulableParam, Accumulable}
import org.apache.commons.io.FilenameUtils


@serializable class Utils {
"/disk1/sdl/softwares/hadoop/etc/hadoop/hdfs-site.xml"

   def checkFileExistInHdfs1(fpath:String):Boolean={
      var hdfs = FileSystem.get(new Configuration())
      var isExists:Boolean = hdfs.exists(new Path(fpath))
      if(isExists)
         return true
         else
         return false
   }
   
   def checkFileExistInHdfs(fpath:String):Boolean={
      var conf = new Configuration()
      var coreSitePath = new Path("/disk1/sdl/softwares/hadoop/etc/hadoop/core-site.xml")
      var hdfsSitePath  = new Path("/disk1/sdl/softwares/hadoop/etc/hadoop/hdfs-site.xml")
      conf.addResource(coreSitePath)
      conf.addResource(hdfsSitePath)
      var hdfs = FileSystem.get(conf)
      var isExists:Boolean = hdfs.exists(new Path(fpath))
      if(isExists)
         return true
         else
         return false
   }
         
       
   //Accumulator Construction 
   implicit object PatientsHashMap extends AccumulatorParam[HashMap[Long, String]] {
      def zero(m: HashMap[Long, String]) = HashMap()
      def addInPlace(m1: HashMap[Long, String], m2: HashMap[Long, String]) = m1 ++ m2 
   }
         

         
   def getSparkContext():SparkContext = {
      val conf = new SparkConf().setAppName("SdlJob1")
      conf.set("spark.kryoserializer.buffer.max","2000m")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.driver.maxResultSize", "0")
      conf.set("spark.rdd.compress", "true")
      //conf.set("spark.storage.memoryFraction", "1")//we commented on 08-02-2016
      conf.set("spark.core.connection.ack.wait.timeout", "600")
      conf.set("spark.akka.frameSize", "300")
      conf.set("spark.akka.timeout", "300")
      conf.set("spark.storage.blockManagerSlaveTimeoutMs", "300000")
//I added below two lines on 08-02-2016
      conf.set("spark.storage.memoryFraction", "0.8")
      conf.set("spark.shuffle.memoryFraction", "0.2")
//Related to below line I did modicication on /disk1/sdl/softwares/spark/conf/spark-env.sh . I added below two lines on 08-02-2016
      //conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

      val sc = new SparkContext(conf)
      sc
   }
         
         
   def lsRDFParser ( lineStr : String) : Array[String] = {
      var lineData = lineStr.trim
      var tripleArr = new Array[String](4)
      if(lineData != ""){
         var triple = lineData.mkString.split(">\\s+")       // split on "> "
         tripleArr(0) = triple(0).trim.substring(1)   // substring() call
         tripleArr(1) = triple(1).trim.substring(1) // to remove "<"
         if(triple(2).trim.substring(0,1) == "<")
            tripleArr(2) = triple(2).trim.substring(1)  // Lose that <.
            else
            tripleArr(2) = triple(2).trim
         
         if(FilenameUtils.getName(tripleArr(1)) == "cdm#demography"){                  
               tripleArr(3) = "Patient"
         }else {
               tripleArr(3) = "Ordinary"
         }              
      }
      tripleArr
   }



//This is a string accumulator but it is not using currently
	implicit object StringAccumulatorParam extends AccumulatorParam[String] {

	    def zero(initialValue: String): String = {""}

	    def addInPlace(s1: String, s2: String): String = {s"$s1 $s2"}
	}

   

}
