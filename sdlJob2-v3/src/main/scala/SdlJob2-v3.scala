import scala.io.Source 
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import java.io.PrintWriter
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
import palg.pAlg
import java.net._
import scala.collection.JavaConversions._
import java.util.Calendar
import org.apache.spark.storage.StorageLevel._



object GraphGenerationFromEdgeFile{
	def main(args: Array[String]) {
   
   var startTime = Calendar.getInstance().getTime()
   println("********************** SDL-ETL-Job-2.v3 Started **********************")
   var utils:Utils =new Utils
   var palgObj:pAlg =new pAlg
   val sc = utils.getSparkContext()
	
   try{
      if(args.length < 2){
         println("\n\n\nInsufficient number of arguments received \nIt required below mentioned items as arguemnets \n 1. Input file name \n 2. No. of Rdd partitions \n\n\n")
      }else{

         var activeDir: EdgeDirection = EdgeDirection.Out
         var maxIterations = Int.MaxValue //Int.MaxValu(2147483647)        
         val localhost = InetAddress.getLocalHost
         var hostName = localhost.getHostName
         var hdfsUserName = "tarak"
         var port = "9000"
         var primaryPatientsFileName = "Dist-"+args(0)
         var edgeFileName = "Edg-"+args(0)
         
         var noOfPartitions = sc .defaultParallelism
         if(args.length > 1)            
                 noOfPartitions = args(1).toInt

                 
         var inputFolderPath = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/output/"
         var outputFolderPath = "hdfs://"+hostName+":"+port+"/user/"+hdfsUserName+"/output/"
         var uniquePatientsIdsFilePath:String = inputFolderPath+primaryPatientsFileName.toString()
 
         var baseFileName:String = FilenameUtils.getBaseName(inputFolderPath+edgeFileName)
         var baseFileExtension:String = FilenameUtils.getExtension(inputFolderPath+edgeFileName)
         

         
/******************************* Check for input and output file in hdfs *******************************/
        if(!utils.checkFileExistInHdfs(inputFolderPath + edgeFileName)){
               println("\n\n\n Input file doesnt exist: " + inputFolderPath + edgeFileName +" \n\n\n")
         }else if(!utils.checkFileExistInHdfs(uniquePatientsIdsFilePath)){
               println("\n\n\n Input file doesnt exist: " + uniquePatientsIdsFilePath +" \n\n\n")
         }else if(utils.checkFileExistInHdfs(outputFolderPath+"Graph-output-for-"+baseFileName+".nq")){
               println("\n\n\n Output file already Exist: "+outputFolderPath+"Graph-output-for-"+baseFileName+".nq \n\n\n")
         }else{

          println(uniquePatientsIdsFilePath)
          val primaryPatientsRdd = sc.textFile(uniquePatientsIdsFilePath, noOfPartitions).filter(_.toString != "").map { line =>               
               var primaryPatientData = line.mkString.split("\\t")
               (primaryPatientData(0).toLong,primaryPatientData(1))
          }          
          val bcuniqPPHmap = sc.broadcast(primaryPatientsRdd.collectAsMap())
          
          val triplets = sc.textFile(inputFolderPath+edgeFileName, noOfPartitions).filter(_.toString != "None").map { line =>
               var edgeData = line.mkString.split("\\t") 
               val t = new EdgeTriplet[Long, Long]
               t.srcId = edgeData(0).toLong
               t.dstId = edgeData(1).toLong              
               
               
               if(edgeData(5).equalsIgnoreCase("Patient"))
                  t.attr = edgeData(6).toLong
               else
                  t.attr = edgeData(6).toLong
               
               t.dstAttr = -1L
               if(bcuniqPPHmap.value.contains(t.srcId)){
                  t.srcAttr = t.srcId
               }else{
                  t.srcAttr = -1
               }
               /*
               if(edgeData(5).equalsIgnoreCase("Patient")){
                  t.srcAttr = t.srcId
               }else {
                  if(t.srcAttr < 0L)   // There will many but only one triple having patient label
                     t.srcAttr = -1L
                     else
                     t.srcAttr = t.srcId
               }
               */
               t 
           }          
/******************************* Graph Construction *******************************/          
          val vertices = triplets.flatMap(t => Array((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))  
          println("Vertices construction completed")
          
          val edges = triplets.map(t => t: Edge[Long])
          println("Edges construction completed")          
          
          val graph = Graph(vertices, edges).cache
          println("Graph construction completed")
          
          
/******************************* Pregel code *******************************/          
          println("Graph algorithm executing...")
          val multiLeval = palgObj.getPAlgResult(graph,maxIterations,activeDir)
          println("Graph algorithm completed")               
          

/******************************* Output Rdf Generation *******************************/          
          var pregelOutputNQuadsRdd = palgObj.getNquadOutputRdd(multiLeval)
          println("Graph algorithm output rdd generation Completed")
          
         
          pregelOutputNQuadsRdd.filter(_.toString != "").saveAsTextFile(outputFolderPath+"Graph-output-for-"+baseFileName+".nq")
          println("Writing Graph algorithm output rdd to HDFS completed")
          
          
          println("Graph Triplets count: "+graph.triplets.count)
          val outputTriplets = sc.textFile(outputFolderPath+"Graph-output-for-"+baseFileName+".nq")
          println("Output Triplets count: "+outputTriplets.count)
          
          graph.unpersist()
          
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
            println("********************** SDL-ETL-Job-2.v3 Completed **********************")
      }
   }
}

