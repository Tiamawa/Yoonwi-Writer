package com.sonatel.yoonwi.classes

import com.sonatel.yoonwi.utils.{GPS, Troncon}
import org.apache.spark.sql.{Row, SparkSession}
import java.util

class TronconFileReader(spark : SparkSession, path:String) {

  def reader():  List[Troncon]={
    var troncons = List[Troncon]()

    var i =0

    val tronconDF = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)

    /**
      * Transformation de la dataframe en une liste
      */
    val tronconList: util.List[Row] = tronconDF.collectAsList()//return a java.util.List[Row]

    val size = tronconList.size()//List size


    for(i<-0 until size){
      var agP = tronconList.get(i)

      var points = List[GPS]()

      var id = agP.getString(0).toInt //id du troncon

      var voie = agP.getString(1).toInt //nombre de voies

      //var a1 = agP.getString(2).split(",")
      var p1 = new GPS(agP.getString(2).toDouble, agP.getString(3).toDouble)//premier point du polygone

      //var a2 = agP.getString(2).split(",")
      var p2 = new GPS(agP.getString(4).toDouble, agP.getString(5).toDouble)//deuxième point du polygone

      //var a3 = agP.getString(3).split(",")
      var p3 = new GPS(agP.getString(6).toDouble, agP.getString(7).toDouble)//troisième point du polygone

     // var a4 = agP.getString(4).split(",")
      var p4 = new GPS(agP.getString(8).toDouble, agP.getString(9).toDouble)//quatrième point du polygone

      var longueur = agP.getString(10).toInt //longueur du troncon

      var vitesseLibre = agP.getString(12).toInt //vitesse libre

      var vitesseCritique = agP.getString(13).toInt //vitesse critique

      var capacite = agP.getString(14).toInt //capacite du troncon

      var concentrationCritique = agP.getString(15).toInt //concentration critique

      points = p1 :: points
      points = p2 :: points
      points = p3 :: points
      points = p4 :: points
      points = p1 :: points //close polygon

      var ag = new Troncon(id, voie, capacite, concentrationCritique, vitesseLibre, vitesseCritique, longueur, points)

      troncons = ag :: troncons

    }
    troncons

  }
}
