package com.sonatel.yoonwi.classes

import com.mongodb.spark._
import org.apache.spark.sql.DataFrame

class Inserter(etats : DataFrame){

	def insert() : Unit = {

		MongoSpark.save(etats.write.mode("append"))

	}
}
