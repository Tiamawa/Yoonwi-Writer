package com.sonatel.yoonwi.classes

import com.sonatel.yoonwi.utils.GPS
import org.apache.spark.sql.{DataFrame, SparkSession}

class Verifier(spark : SparkSession, troncons:DataFrame, vehicules : DataFrame){

  troncons.createOrReplaceTempView("troncon")
  vehicules.createOrReplaceTempView("vehicule")

  def check(): DataFrame ={

    /*UDF pour vérifier la présence du point ou véhicule à l'intérieur du tronçon : winding number*/
    val contains=(vLatitude : Double, vLongitude : Double, latitudePO : Double,
                  longitudePO : Double, latitudePE : Double, longitudePE : Double,
                  latitudeDE : Double, longitudeDE : Double, latitudeDO : Double, longitudeDO : Double)  =>{

     /* def isLeft(point1 : GPS, point2 : GPS, v : GPS): Int = {

        /*tests if a point is Left|On|Right of an infinite line(p0,p1)
      * > 0 for p2 left of the line through p0 and p1
      * = 0 for p2 on the line
      * < 0 for p2 right of the line*/
        val det = (point2.latitude - point1.latitude) * (v.longitude- point1.longitude) - (v.latitude-point1.latitude) * (point2.longitude - point1.longitude)
        if (det < 0) -1
        else if (det > 0) +1
        else 0
      }*/
      /* These functions test for even and odd numbers.*/
      def isEven(number: Int) : Boolean = number % 2 == 0
     // def isOdd(number: Int) : Int = !isEven(number) //impair

      var troncon = List[GPS]()
      val vehicule = new GPS(vLatitude, vLongitude)
      var crossing = 0 //compteur de l'indice


      val po = new GPS(latitudePO, longitudePO)
      val pe = new GPS(latitudePE, longitudePE)
      val de = new GPS(latitudeDE, longitudeDE)
      val dorigine = new GPS(latitudeDO, longitudeDO)
      //constitution de la liste
      troncon = po :: troncon
      troncon = pe :: troncon
      troncon = de :: troncon
      troncon = dorigine :: troncon
      //Fermeture du polygone : le dernier point doit être égale au premier point du polygone
      troncon = po :: troncon
      var cn =0/*Crossings number*/
      for (i <- 0 to troncon.size - 2) {
        if(((troncon(i + 1).longitude > vehicule.longitude) && (vehicule.longitude >= troncon(i).longitude)) //upward crossing
          ||
          ((troncon(i + 1).longitude <= vehicule.longitude) && (vehicule.longitude < troncon(i).longitude)) //a downward crossing
        ){
          //calculer la coordonnÃ©e x intersectÃ©e rÃ©elle
          var vt = ((vehicule.longitude - troncon(i).longitude) / (troncon(i + 1).longitude - vehicule.longitude)).toFloat
          if (vehicule.latitude < (troncon(i).latitude + vt * (troncon(i + 1).latitude - troncon(i).latitude))) // P.x < intersect
            cn += 1
        }
        }
      cn

      }
	  //UDF : calcul du milieu  de chaque balise
    val milieu = (element1 : Double, element2 : Double)=>{
      (element1+element2)/2
    }

      /*if(windings != 0){
        if(nbVehicules > 0 && vMoyenne >= vitesseLibre){
        etat = "Fluide"
      }
        if(nbVehicules > 0 && nbVehicules <= concentrationCritique){
          if(vMoyenne < vitesseLibre && vMoyenne >= vitesseCritique){
            etat="Trafic"
          }
        }
        if(nbVehicules >= concentrationCritique && nbVehicules < capacite){
          if(vMoyenne < vitesseCritique){
            etat="Congestion"
          }
        }
        if(nbVehicules==capacite && vMoyenne ==0){
          etat="Bouchon"
        }
        etat
      }*/

    /*Registering udf*/
    spark.sqlContext.udf.register("winding", contains)
    spark.sqlContext.udf.register("halfing", milieu)

	/*UDF : déduction des états des tronçons à partir de la vitesse moyenne et de la densité*/
    val etats = (concentrationCritique : Int, capacite : Int, nbVehicules : Int, vitesseLibre : Int, vitesseCritique : Int, vMoyenne : Double)=>{

      var etat ="No data"
      if(nbVehicules < concentrationCritique/2 && vMoyenne >= vitesseLibre){ //Fluide : peu e véhicules roulant à vive allure
        etat = "Fluide"
      }
      if(nbVehicules < concentrationCritique/2 && vMoyenne <= vitesseLibre){ //Fluide : peu de véhicules roulant à une vitesse maintenue
        etat = "Fluide"
      }
      if(nbVehicules < concentrationCritique/2 && vMoyenne <= vitesseCritique){ //Fluide : peu de véhicules roulant à une vitesse très faible
        etat = "Fluide"
      }
      if(nbVehicules > concentrationCritique/2 && nbVehicules <= concentrationCritique){
        if(vMoyenne < vitesseLibre && vMoyenne >= vitesseCritique){
          etat="Trafic"
        }
      }
      if(nbVehicules >= concentrationCritique && nbVehicules < capacite){
        if(vMoyenne < vitesseCritique){
          etat="Congestion"
        }
      }
      if(nbVehicules >= capacite){
        etat="Bouchon"
      }
      etat
    }

	/*Registering udf */
    spark.sqlContext.udf.register("etat", etats)
    val nbVehicules = (winding : Double)=>{

    }

    /**
      * Récupération de densites
      */
   /*val  densites = spark.sql("SELECT first_value(t.tronconId) as troncon,first_value(t.voie) as voies, first_value(t.longueur) as longueur, first_value(t.capacite) as capacite, first_value(t.concentrationCritique) as concentrationCritique, " +
     " count(*) as nbVehicules, first_value(t.vitesseLibre) as vLibre, first_value(t.vitesseCritique) as vCritique, avg(v.speed) as vMoyenne FROM troncon t, vehicule v" +
     " WHERE (winding(v.latitude, v.longitude, t.latitudePO, t.longitudePO, t.latitudePE, t.longitudePE, t.latitudeDE, t.longitudeDE, t.latitudeDO, t.longitudeDO)%2 <> 0)")*/

    val  densites = spark.sql("SELECT t.tronconId as tronconId, t.voie as voies, t.longueur as longueur, t.capacite as capacite, t.concentrationCritique as concentrationCritique, count(v.speed) as nbVehicules, " +
     "t.vitesseLibre as vLibre, t.vitesseCritique as vCritique, avg(v.speed) as vMoyenne, ((t.latitudePO+t.latitudeDO)/2) as latitudeMO, ((t.longitudePO+t.longitudeDO)/2) as longitudeM0," +
     "((t.latitudePE+t.latitudeDE)/2) as latitudeME, ((t.longitudePE+t.longitudeDE)/2) as longitudeME," +
     "(etat(t.concentrationCritique, t.capacite, count(v.speed), t.vitesseLibre, t.vitesseCritique, avg(v.speed))) as etat  FROM troncon t, vehicule v" +
      " WHERE (winding(v.latitude, v.longitude, t.latitudePO, t.longitudePO, t.latitudePE, t.longitudePE, t.latitudeDE, t.longitudeDE, t.latitudeDO, t.longitudeDO)%2 <> 0)" +
     " GROUP BY t.tronconId, t.voie, t.longueur, t.capacite, t.concentrationCritique, t.vitesseLibre, t.vitesseCritique, t.latitudePO, t.longitudePO, t.latitudeDO, t.longitudeDO, t.latitudePE, t.longitudePE, t.latitudeDE, t.longitudeDE")

    //densites.createOrReplaceTempView("densites")
    /**
      * Déduction des états des tronçons
      */
   /* val etatsFuildes : DataFrame= spark.sql("SELECT troncon, voies, longueur, nbVehicules, vLibre, vCritique, vMoyenne, capacite, concentrationCritique FROM densites " +
      " WHERE dActuel > 0  AND vMoyenne >= vLibre")
    val tronconsFluides = etatsFuildes.withColumn("etat",lit("Fluide"))

    val etatsNormaux : DataFrame = spark.sql("SELECT troncon, voies, longueur, nbVehicules, vLibre, vCritique, vMoyenne, capacite, concentrationCritique FROM densites " +
      " WHERE dActuel > 0 AND dActuel <= concentrationCritique AND vMoyenne < vLibre AND vMoyenne >= vCritique")
    val tronconsNormaux = etatsNormaux.withColumn("etat",lit("Trafic"))

    val etatsCongestiones = spark.sql("SELECT troncon, voies, longueur, nbVehicules, vLibre, vCritique, vMoyenne, capacite, concentrationCritique FROM densites " +
      "WHERE dActuel >= concentrationCritique AND dActuel < capacite AND vMoyenne < vCritique")
    val tronconsCongestiones = etatsCongestiones.withColumn("etat", lit("Congestion"))

    val etatsEmbouteilles = spark.sql("SELECT troncon, voies, longueur, nbVehicules, vLibre, vCritique, vMoyenne, capacite, concentrationCritique FROM densites " +
      "WHERE dActuel = capacite AND vMoyenne = 0")
    val tronconsBouchons = etatsEmbouteilles.withColumn("etat",lit("Bouchon"))

    val etatsVides = spark.sql("SELECT troncon, voies, longueur, nbVehicules, vLibre, vCritique, vMoyenne, capacite, concentrationCritique FROM densites " +
      "WHERE dActuel = 0 AND vMoyenne = 0")
    val tronconsVides = etatsVides.withColumn("etat", lit("Vide"))

    val etatsTroncons = tronconsFluides.union(tronconsNormaux).union(tronconsCongestiones).union(tronconsBouchons).union(tronconsVides)*/

    /*Return les états des différents tronçons*/
    densites
  }

}
