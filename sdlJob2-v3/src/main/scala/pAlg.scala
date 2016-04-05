package palg

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable.HashMap




@serializable class pAlg() {

   /* Function to run pregal logic on the graph */
   def getPAlgResult(graph: Graph[Long,Long], maxIterations: Int, activeDir: EdgeDirection): Graph[Long,Long] = {   
               val multiLeval = graph.pregel(-2L,maxIterations,activeDir)(
                  (id, attr, newPid) => {
                        math.max(attr, newPid)
                  }, 
                  triplet => {                     
                     if ( triplet.srcAttr > -1L && triplet.dstAttr == -1L ) {
                           Iterator((triplet.dstId, triplet.srcAttr))						
                        } else {
                           Iterator.empty
                        }
                  },
                  (a,b) => {                    
                     a        //It may be either a or b
                  }
               )               
               multiLeval
      
   }
   
   
   
   /* Function to generate nquads for the pregel output */
   //, bcuniqPPHmap: org.apache.spark.broadcast.Broadcast[scala.collection.Map[Long,String]]
   
   def getNquadOutputRdd(multiLeval: Graph[Long,Long]):RDD[String] = {
      var pregelOutputNQuadsRdd = multiLeval.triplets.map(t => {
                  var str = ""
                     if(t.srcAttr > -1L){ // src ID can be 0L
                        var sub = t.srcId
                        var pred = "\t"+t.attr
                        var obj = "\t"+t.dstId
                        var gName = "\t"+t.srcAttr
                        if(str != "")
                           str = str.concat("\n"+sub+pred+obj+gName)
                        else
                           str = sub+pred+obj+gName
                     }
                 
                  str
             })
     pregelOutputNQuadsRdd
   }
   


   
}