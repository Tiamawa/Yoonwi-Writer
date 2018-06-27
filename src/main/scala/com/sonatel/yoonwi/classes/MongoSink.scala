package com.sonatel.yoonwi.classes

import java.util.Calendar

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{ForeachWriter, Row}
import org.bson
import org.bson.Document

import scala.collection.JavaConverters._
import scala.collection.mutable

class MongoSink extends ForeachWriter[Row] {

    val writeConfig: WriteConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1:27017/yoonwi.etats"))
    var mongoConnector: MongoConnector = _
    var etatsDates: mutable.ArrayBuffer[Row] = _

    def process(value: Row): Unit = {

      etatsDates.append(value)
    }


    def close(errorOrNull: Throwable): Unit = {
      if (etatsDates.nonEmpty) {
        mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>


          collection.insertMany(etatsDates.map(ed => {

           // val now = Calendar.getInstance()
           // var values = scala.collection.Map[String, Object]()

            //var doc = new bson.Document()

           new Document().append("id", ed(0)).append("nbVehicules",ed(5)).append("vMoyenne" , ed(8)).append("latitudeMO" , ed(9)).append("longitudeMO" , ed(10))
               .append("latitudeME", ed(11)).append("longitudeME" , ed(12)).append("etat" , ed(13)).append("heure", Calendar.getInstance().get(Calendar.HOUR_OF_DAY)).append("minute", Calendar.getInstance().get(Calendar.MINUTE))

            /*values += ("id" -> ed(0))
            values += ("voies" -> ed(1))
            values += ("longueur" -> ed(2))
            values += ("capacite" -> ed(3))
            values += ("concentrationCritique" -> ed(4))
            values += ("vitesseLibre" -> ed(6))
            values += ("vitesseCritique" -> ed(7))
            values += ("vitesseMoyenne" -> ed(8))
            values += ("etat" -> ed(9))*/


            //val v = Array(ed(0),ed(1),ed(2),ed(3),ed(4),ed(5),ed(6), ed(7),ed(8), ed(9), now.get(Calendar.HOUR_OF_DAY).toString, now.get(Calendar.MINUTE).toString)
            //var value = ed(0)+","+ed(1)+","+ed(2)+","+ed(3)+","+ed(4)+","+ed(5)+","+ed(6)+","+ed(7)+","+ed(8)+","+ed(9)+","+(now.get(Calendar.HOUR_OF_DAY)).toString+","+(now.get(Calendar.MINUTE)).toString
           // new Document(ed(0).toString, value)//Constructor(key:String, value:Object)
          }).asJava)
        })
      }
    }


    def open(partitionId: Long, version: Long): Boolean = {
      mongoConnector = MongoConnector(writeConfig.asOptions)
      etatsDates = new mutable.ArrayBuffer[Row]()
      true
    }
}
