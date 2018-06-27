package com.sonatel.yoonwi.launcher

import com.sonatel.yoonwi.classes.{MongoSink, Reader, Verifier}
import com.sonatel.yoonwi.utils.CustomConfig
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class GroupedEvent(speed : Int, latitude : Double, longitude : Double)

object Writer {

  /*Mise en place d'un logger*/
  val logger : Logger = LogManager.getLogger(getClass)

  val hadoopConfig = new Configuration()


  def main(args: Array[String]): Unit = {

    /**
      * Verifying number of arguments in the command line :
      * Check if configuration file is provided
      */
    if(args.length == 0){
      logger.info("Le programme requiert un fichier de configuration pour s'exécuter")
      System.exit(0)
    }

    /**
      * Retrieving configuration file directory
      */
    val hdpConfigPath = args(0)

    /**
      * Loading configuration file
      */
    CustomConfig.load(hdpConfigPath+"/application.conf")

    /**
      * Retrieving configuration parameters values
      */
    val BOOTSTRAP_SERVERS=CustomConfig.BOOTSTRAP_SERVERS
    val DEFAULT_TRONCONS_PATH=CustomConfig.DEFAULT_TRONCONS_PATH
    val SPARK_WAREHOUSE_PATH=CustomConfig.SPARK_WAREHOUSE_PATH
    val GROUPED_VEHICLES_TOPIC_NAME=CustomConfig.GROUPED_VEHICLES_TOPIC_NAME
    val CHECKPOINT_DIRECTORY=CustomConfig.CHECKPOINT_DIRECTORY

    /*Mongo DB parameters*/
    val OUTPUT_COLLECTION = CustomConfig.OUTPUT_COLLECTION
    val OUTPUT_URI = CustomConfig.OUTPUT_URI

    /**
      * Instantiate Spark Session
      */
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("YOONWI")
      .config("spark.sql.warehouse.dir",SPARK_WAREHOUSE_PATH)
      //Configuration for writing into mongo db collection
      .config("spark.mongodb.output.uri", OUTPUT_URI)
      .config("spark.mongodb.output.collection", OUTPUT_COLLECTION)
      .getOrCreate()

    /**
      * Reading troncons file from HDFS
      */
    val tronconsDF : DataFrame = new Reader(spark, DEFAULT_TRONCONS_PATH).read()

    val troncons = tronconsDF.toDF("tronconId","voie", "latitudePO", "longitudePO", "latitudeDO", "longitudeDO", "latitudePE", "longitudePE",
      "latitudeDE", "longitudeDE", "longueur", "observation", "vitesseLibre", "vitesseCritique", "capacite", "concentrationCritique")

	  /**
	    * Reading grouped vehicles from Kafka
		*/
    val groupedVehiclesFromKafka= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
      .option("subscribe", GROUPED_VEHICLES_TOPIC_NAME)
      .option("startingOffsets","earliest")
      .load()

    object GroupedVehicle{
      def apply(rawStr : String):GroupedEvent = {
        val parts = rawStr.split(",")
        GroupedEvent(Integer.parseInt(parts(0)),java.lang.Double.parseDouble(parts(1)), java.lang.Double.parseDouble(parts(2)))
      }
    }
    import spark.implicits._//For Error : Unable to find encoder for type stord in a Dataset. Primitives ... Error
    val groupedVehicles = groupedVehiclesFromKafka
      .selectExpr("CAST(value AS STRING)")
      .map(r => GroupedVehicle(r.getString(0))).toDF("speed","latitude","longitude")

	  /**
	    * Retrieving trafic state 
		*/
    val etatsTroncons : DataFrame = new Verifier(spark, troncons, groupedVehicles).check()

    //Instantiate a Mongo Sink 
    val mongoSink = new MongoSink()

    //Using mongo sink for writing to mongo db
    val  writing= etatsTroncons.writeStream
        .foreach(mongoSink)
        .outputMode("complete") //append mode not allowed when there are aggreagations on streaming dataframe
     // .format("console")
     // .option("truncate","false")
      .start()
    // .foreach(writer)

    writing.awaitTermination() //Waiting for streaming query to stop

  }

}
