import scala.io.Source 
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.io.PrintWriter
import java.io._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.MutableList

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark._
import scalax.io._
import org.apache.commons.io.FilenameUtils
import utils.Utils
import java.net._
import scala.collection.JavaConversions._
import java.util.Calendar
import org.apache.spark.storage.StorageLevel._
import java.io.File



import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
  

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any = 
      NullWritable.get()
  
    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = 
      key.asInstanceOf[String]
  }


object FinalNquadGeneration{
	def main(args: Array[String]) {
   
     var startTime = Calendar.getInstance().getTime()
     println("********************** SDL-ETL-Job-3.v3 Started **********************")
     var utils:Utils =new Utils
     val sc = utils.getSparkContext()
	
   try{
      if(args.length < 2){
         println("\n\n\nInsufficient number of arguments received \nIt required below mentioned items as arguemnets \n 1. File path \n 2. No. of Rdd partitions \n\n\n")
      }else{

         val localhost = InetAddress.getLocalHost
         var hostName = localhost.getHostName 
         var hdfsUserName = "tarak"
         var port = "9000"
         var primaryPatientsFileName = ""
         var job2job2OutputFileName = ""      
         if(args.length > 0){
              primaryPatientsFileName = "Dist-"+args(0)                 
              job2job2OutputFileName = "job1-job2-output-for-"+args(0)              
         }         
         var noOfPartitions = sc .defaultParallelism
         if(args.length > 1)            
                 noOfPartitions = args(1).toInt          
         var inputFolderPath = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/output/"
         var outputFolderPath = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/output/"
         var uniquePatientsIdsFilePath:String = inputFolderPath+primaryPatientsFileName.toString()            
         var baseFileName:String = FilenameUtils.getBaseName(args(0))         
         var inputEdgFilePath = inputFolderPath+"Edg-"+baseFileName+".txt"
         var inputPregelFilePath = inputFolderPath+"Graph-output-for-Edg-"+baseFileName+".nq"
         var job3OutputFolderPath = outputFolderPath+"patient-files-for-"+baseFileName
                 
/******************************* Check for input and output file in hdfs *******************************/
        if(!utils.checkFileExistInHdfs(inputEdgFilePath)){
               println("\n\n\n Input file doesnt exist: " + inputEdgFilePath +" \n\n\n")
         }else if(!utils.checkFileExistInHdfs(inputPregelFilePath)){
               println("\n\n\n Input file doesnt exist: " + inputPregelFilePath +" \n\n\n")
         }else if(utils.checkFileExistInHdfs(job3OutputFolderPath)){
               println("\n\n\n Output file already exist: " + job3OutputFolderPath +" \n\n\n")
         }else{ 
         
         
         val primaryPatientsRdd = sc.textFile(uniquePatientsIdsFilePath, noOfPartitions).filter(_.toString != "").map { line =>               
             var primaryPatientData = line.mkString.split("\\t")
             (primaryPatientData(0).toLong,primaryPatientData(1))
         }          
            val bcuniqPPHmap = sc.broadcast(primaryPatientsRdd.collectAsMap())
          
            val nquadRdd = sc.textFile(inputEdgFilePath).filter(_.toString != "").map( line => {
            var lineData = line.mkString.split("\\t")
            (lineData(6),(lineData(2),lineData(3),lineData(4)))}).leftOuterJoin(
            sc.textFile(inputPregelFilePath).filter(_.toString != "").map( line => {
            var lineData1 = line.mkString.split("\\t")            
            (lineData1(1),lineData1(3))})).map(t => {
                var str = ""            
                var obj = ""
                var graphName = ""
                if(t._2._1._3.trim.substring(0,1) == "\"")
                   obj =  t._2._1._3+"> "
                   else
                   obj =  "<"+t._2._1._3+"> "
               var pUri = bcuniqPPHmap.value(t._2._2.get.toLong)
               var gArr = pUri.split("/")
               graphName = gArr(gArr.length-1).concat(".nq")            
               str = str.concat("<"+t._2._1._1+"> <"+t._2._1._2+"> "+obj+" <"+pUri+"> .")
               (graphName,str)
            }).cache            
            println("nquadRdd completed")
            var oPath = job3OutputFolderPath+args(0).toString
            nquadRdd.map(a => (a._1, a._2)).partitionBy(new HashPartitioner(noOfPartitions)).saveAsHadoopFile(oPath, classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
            nquadRdd.unpersist(true)

         
           //http://www.unknownerror.org/opensource/apache/spark/q/stackoverflow/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job
           //http://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job
        }
      }
         
      } catch {
         case e: Exception => println("\n\nException caught: " + e+"\n\n")
      }finally {      
            sc.stop()
            println("Spark context stopped")
            var endTime = Calendar.getInstance().getTime()
            println("Start Time: "+startTime)
            println("  End Time: "+endTime)
            println("********************** SDL-ETL-Job-3.v3 Completed **********************")
      }
   }
}

