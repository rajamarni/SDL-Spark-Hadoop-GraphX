package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs
import java.io._
import scalax.io._

import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._




class Utils {

      def constructKey(pId:Any, nId:Any):String={
			   pId.toString.concat("~"+nId.toString)
		}
      
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
         
         def createDir(folderName:String):String = {
            var conf = new Configuration()
            var coreSitePath = new Path("/disk1/sdl/softwares/hadoop/etc/hadoop/core-site.xml")
            var hdfsSitePath  = new Path("/disk1/sdl/softwares/hadoop/etc/hadoop/hdfs-site.xml")
            conf.addResource(coreSitePath)
            conf.addResource(hdfsSitePath)
            var hdfs = FileSystem.get(conf)

            var homeDir=hdfs.getHomeDirectory()
            var workingDir=hdfs.getWorkingDirectory()
            var newFolderPath= new Path("/output/"+folderName)
            newFolderPath=Path.mergePaths(workingDir, newFolderPath)

            if(hdfs.exists(newFolderPath)){
               hdfs.delete(newFolderPath, true) //Delete existing Directory
            }
            hdfs.mkdirs(newFolderPath)     //Create new Directory
            newFolderPath.toString
         }
         
         
         def getSparkContext():SparkContext = {
            val conf = new SparkConf().setAppName("FinalNquadGeneration")
            conf.set("spark.kryoserializer.buffer.max","2000m")
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            conf.set("spark.driver.maxResultSize", "0")
            conf.set("spark.rdd.compress", "true")
            conf.set("spark.core.connection.ack.wait.timeout", "600")
            conf.set("spark.akka.frameSize", "300")
            conf.set("spark.akka.timeout", "300")
            conf.set("spark.storage.blockManagerSlaveTimeoutMs", "700000")
            val sc = new SparkContext(conf)
            sc
         }
        
          

}
