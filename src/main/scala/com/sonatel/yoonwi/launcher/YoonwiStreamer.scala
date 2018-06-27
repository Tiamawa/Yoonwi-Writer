package com.sonatel.yoonwi.launcher

import com.sonatel.yoonwi.classes.{DStreamReader, InPointsFinder, KafkaParamsInitializer, TronconFileReader}
import com.sonatel.yoonwi.utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object YoonwiStreamer {


  /*def main(args:Array[String]):Unit={
    if (args.length == 0) {
      println("Configuration file required")
      System.exit(0)
    }

    /**
      * Récupération du fichier de configuration dans HDP
      */
    val hdpConfigPath = args(0)


    /**
      * Chargement du fichier de configuration
      */
    CustomConfig.load(hdpConfigPath + "/application.conf")

    /**
      * Retrieving configuration parameters values
      */
    val BOOTSTRAP_SERVERS=CustomConfig.BOOTSTRAP_SERVERS
    val DEFAULT_TRONCONS_PATH=CustomConfig.DEFAULT_TRONCONS_PATH
    val SPARK_WAREHOUSE_PATH=CustomConfig.SPARK_WAREHOUSE_PATH
    val GROUPED_VEHICLES_TOPIC_NAME=CustomConfig.GROUPED_VEHICLES_TOPIC_NAME
    val ACKS = CustomConfig.ACKS
    val KEY_SERIALIZER_CLASS = CustomConfig.KEY_SERIALIZER_CLASS
    val VALUE_SERIALIZER_CLASS = CustomConfig.VALUE_SERIALIZER_CLASS
    val STREAMING_WINDOW_VALUE = CustomConfig.STREAMING_WINDOW_VALUE
    val STREAMING_GROUP_ID=CustomConfig.STREAMING_GROUP_ID
    val CHECKPOINT_DIRECTORY=CustomConfig.CHECKPOINT_DIRECTORY

    /*Mongo DB parameters*/
    val OUTPUT_COLLECTION = CustomConfig.OUTPUT_COLLECTION
    val OUTPUT_URI = CustomConfig.OUTPUT_URI

    /********************************************
      * *************************************************** Initialisation de variables
      */

    /**
      * Instantiation d'un tableau vide de points, d'agences et d'une variable de boucle
      */
    var points = List[GPS]()
    var troncons = List[Troncon]()
    var vehiculesPresents=List[VehiculePresent]()
    var machins = List[Machin]()
    var i =0

    /**
      * Topics name will be read
      */
    val topics = Set(GROUPED_VEHICLES_TOPIC_NAME)

    /**
      * Spark Session
      */
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("YOONWI")
      .config("spark.sql.warehouse.dir",SPARK_WAREHOUSE_PATH)
      //Writing into mongo db collection
      .config("spark.mongodb.output.uri", OUTPUT_URI)
      .config("spark.mongodb.output.collection", OUTPUT_COLLECTION)
      .getOrCreate()


    /**
      * Récupération fichier des agences en un dataframe
      */
    troncons = new TronconFileReader(spark,DEFAULT_TRONCONS_PATH).reader()

    /*Streaming Context*/
    val ssc = new StreamingContext(spark.sparkContext, Seconds(STREAMING_WINDOW_VALUE))
    /**
      * Kafka params
      */

    val kafkaParams = new KafkaParamsInitializer(BOOTSTRAP_SERVERS, STREAMING_GROUP_ID, ssc, topics).kafkaParams()

    /**
      * Kafka direct stream creation
      */
    val directStream = new DStreamReader(ssc,topics,kafkaParams).directStreamCreator()

    /**********************************************
      **************************************************  Traitement
      */
    var value: String = ""
    var vehicule: Array[String] = null
    var latitude = 0.0
    var longitude = 0.0
    var vitesse = 0
/* statuses : DStream arr= new ArrayBuffer{String]
    statuses.foreachRDD {
      arr ++= _.collect() //you can now put it in an array or d w/e you want with it
        ...
    }*/

    directStream.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreach { record =>
          value = record.value()

          /*La valeur est splité en fonction du ";" */
          vehicule = value.split(",")

          try {

            vitesse = vehicule(0).toInt
            latitude = vehicule(1).toDouble
            longitude = vehicule(2).toDouble
            /**
              * Vérification des champs latitude et longitude du rdd récupéré
              */
            if (latitude.isInstanceOf[Double] && longitude.isInstanceOf[Double]) {
              /**
                * Récupération des informations du vehicule
                */
              val v = new Vehicule(vitesse, new GPS(latitude, longitude))
              /**
                * Parcours de la liste des troncons et application de l'algoritme pour chaque vehicule
                */
               var nbVeicules=0 //Initialisation du compteur de véhicules

              for (i <- 0 to (troncons.size - 1)) {

                var response = new InPointsFinder(v, troncons(i)).pointIn()

                if(response==true){ //le véhicule est dans le troncon
                  nbVeicules+=1 //incrémentation du nombre de véhicules
                  val vp  = new VehiculePresent(troncons(i), v)
                  vehiculesPresents = vp :: vehiculesPresents
                }
              }
              }
          } catch {
            case nfe: NumberFormatException => println("Format numérique incorrect")
            case ioobe: IndexOutOfBoundsException => println("Indice trop grand " + ioobe.printStackTrace())
            case e: Exception => println("An exception occcured" + e.printStackTrace())
          }
        }
      }
    )//après le foreachRDD
    ssc.start()
    ssc.awaitTerminationOrTimeout(60000)
    ssc.stop()

    for(i<-0 to vehiculesPresents.size-1){
      var vMoyenne = 0.0
      var vitesseSomme = 0
      var nbVehicules = 0
      for(j<- i+1 to vehiculesPresents.size-1){
        if(vehiculesPresents(i).troncon.id == vehiculesPresents(j).troncon.id){
          vitesseSomme=vehiculesPresents(i).v.vitesse+vehiculesPresents(j).v.vitesse
          nbVehicules+=1 //incrémente !
        }
      }
      //fin pour chaque i
      vMoyenne=vitesseSomme/nbVehicules
      val m = new Machin(vehiculesPresents(i).troncon, nbVehicules, vMoyenne)

      machins = m :: machins


    }
  }*/
}
