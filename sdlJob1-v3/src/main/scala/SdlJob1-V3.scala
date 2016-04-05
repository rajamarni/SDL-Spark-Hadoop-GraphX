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
import java.io._
import scalax.io._
import org.apache.commons.io.FilenameUtils
import utils.Utils
import java.net._
import scala.collection.JavaConversions._
import org.apache.spark.{AccumulableParam, Accumulable}
import java.util.Calendar


object RddParser{
  def main(args: Array[String]) {
     var startTime = Calendar.getInstance().getTime()
     var utils:Utils =new Utils
     val sc = utils.getSparkContext()
     try{
      if(args.length < 1){
        println("\n\n\nInsufficient number of arguments received \nIt required below mentioned items as arguemnets \n 1. Input triples file name \n\n\n")
      }else{
        
         val localhost = InetAddress.getLocalHost
         var hostName = localhost.getHostName
         var hdfsUserName = "franz"
         var port = "9000"
         var inputFilePath:String = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/input/"+args(0).toString()
         var outputFolderPath = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/output/"       
         var baseFileName:String = FilenameUtils.getBaseName(inputFilePath)
         var baseFileExtension:String = FilenameUtils.getExtension(inputFilePath)
        
        /******************************* Check for input and output file in hdfs *******************************/
        
        if(!utils.checkFileExistInHdfs(inputFilePath)){
           println("\n\n\n Input file not Exist \n\n\n")
         }else if(utils.checkFileExistInHdfs(outputFolderPath+"Edg-"+baseFileName+".txt")){
           println("\n\n\n Output File already Exist: "+outputFolderPath+"Edg-"+baseFileName+".txt \n\n\n")             
         }else if(utils.checkFileExistInHdfs(outputFolderPath+"Dist-"+baseFileName+".txt")){
           println("\n\n\n Output File already Exist: "+outputFolderPath+"Dist-"+baseFileName+".txt \n\n\n")             
         } else { 
           
           println("********************** SDL-ETL-Job-1.v3 Started **********************")
           var noOfPartitions = sc .defaultParallelism //return default partitions
          
           if(noOfPartitions < args(1).toInt)
             noOfPartitions = args(1).toInt
          
            println("Reading input data...")
            val inputRdd = sc.textFile(inputFilePath, noOfPartitions).flatMap(line => { 
            if(line != "")
               Some(utils.lsRDFParser(line.toString))
              else
               None
            }).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)//.cache
            println("Reading input data completed")
          
            val uniqVertRdd = inputRdd.map(t => t(0)).union(inputRdd.map(t => t(2))).distinct().zipWithUniqueId      
            val bcuniqVHmap = sc.broadcast(uniqVertRdd.collectAsMap())      
            var primaryPatients = sc.accumulator(new HashMap[Long, String])(utils.PatientsHashMap)
            val inputRdd1 = inputRdd.zipWithUniqueId
           
            println("Preparing Edge data in process...")
            val edgesToFile = inputRdd1.map(tp =>{
            var t = tp._1
            var zipId = tp._2
            if(t(2) != null){
            var subVertexId = bcuniqVHmap.value(t(0))
            if(t(3) == "Patient"){
              var pHash:HashMap[Long, String] = HashMap()
              pHash(subVertexId.toLong) = t(0)
              primaryPatients += pHash
            }
             subVertexId +"\t" +bcuniqVHmap.value(t(2)) +"\t" + t(0) + "\t" + t(1) + "\t" + t(2) + "\t" + t(3)+ "\t" + zipId
            }else{
             None
            }
            })          
          
            edgesToFile.saveAsTextFile(outputFolderPath+"Edg-"+baseFileName+".txt")
            println("Preparing Edge data completed")
            
            println("Preparing distinct patients data in process...")
            var primaryPatientsRdd = sc.parallelize(primaryPatients.value.toSeq)
            val primaryPatientsRddToTextFileRdd = primaryPatientsRdd.map( t => {
              var str = t._1+"\t"+t._2
              str
            })
            primaryPatientsRddToTextFileRdd.saveAsTextFile(outputFolderPath+"Dist-"+baseFileName+".txt")
            println("Preparing distinct patients data in completed")        
            inputRdd.unpersist()
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
           println("********************** SDL-ETL-Job-1.v3 Completed **********************")
     }
   }
}

