package com.sonatel.yoonwi.utils

class Troncon (var id : Int, var voie : Int, var capacite : Int, var concentrationCritique : Int, var vitesseLibre : Int, var vitesseCritique : Int, var longueur : Int, var tr : List[GPS]) extends Serializable {

  override def toString: String = super.toString
}

/**
  * Compagnon object
  */
object Troncon{

  /**
    * Does this polygon contains the point P
    * Référence http://softsurfer.com/Archive/algorithm_0103/algorithm_0103.html : crossings
    * @param v GPS
    * @param t Troncon
    */
  def cnContains(v : Vehicule, t : Troncon): Boolean ={

    /* Close the polygon */
    //ag.closePolygon()

    var cn = 0 /*Crossings counter*/

    for (i <- 0 to (t.tr.size - 2)) {
      if (
        ((t.tr(i + 1).longitude > v.coordonnees.longitude) && (v.coordonnees.longitude >= t.tr(i).longitude)) //upward crossing
          ||
          ((t.tr(i + 1).longitude <= v.coordonnees.longitude) && (v.coordonnees.longitude < t.tr(i).longitude)) //a downward crossing
      ) {
        //calculer la coordonnÃ©e x intersectÃ©e rÃ©elle
        var vt = ((v.coordonnees.longitude - t.tr(i).longitude) / (t.tr(i + 1).longitude - v.coordonnees.longitude)).toFloat
        if (v.coordonnees.latitude < (t.tr(i).latitude + vt * (t.tr(i + 1).latitude - t.tr(i).latitude))) // P.x < intersect
          cn += 1
      }
    }

    isOdd(cn)
  }

  /* These functions test for even and odd numbers.*/
  def isEven(number: Int) : Boolean = number % 2 == 0
  def isOdd(number: Int) : Boolean = !isEven(number)

}
