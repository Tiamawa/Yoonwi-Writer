package com.sonatel.yoonwi.classes

import com.sonatel.yoonwi.utils.{Troncon, Vehicule}

/**
  * This class aim to find vehicles located in an 'troncon'
  */
class InPointsFinder(v : Vehicule, t : Troncon) {

  /**
    *
    * @return Boolean
    */
  def pointIn(): Boolean ={


    var response : Boolean= Troncon.cnContains(v,t)

    response
  }
}
