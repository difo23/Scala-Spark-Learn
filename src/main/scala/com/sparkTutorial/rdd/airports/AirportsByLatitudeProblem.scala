package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark._

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("AirportByLatitudeProblem").setMaster("local[3]")
      val sc = new SparkContext(conf)

      val airportsList = sc.textFile("in/airports.text")
      val airportsByLatitude = airportsList.filter(airport => airport.split(Utils.COMMA_DELIMITER)(6).toFloat>=40)

      val airpotsWithLatitudeMoreThanForty = airportsByLatitude.map(line => {
        val split = line.split(Utils.COMMA_DELIMITER)
        split(1)+": "+split(6)
      })
    airpotsWithLatitudeMoreThanForty.saveAsTextFile("out/airports_with_latitud_more_than_forty")
  }
}
