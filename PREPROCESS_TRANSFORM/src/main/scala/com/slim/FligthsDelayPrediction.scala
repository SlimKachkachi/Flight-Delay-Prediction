package com.slim

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import java.nio.file._


object FligthsDelayPrediction {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(s"WordCount")
      //.master("spark://localhost:7077")
      //.config("spark.hadoop.fs.defaultFS", "hdfs://vmhadoopmaster:9000") essai lamsade
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")


    val currentPath = Paths.get("").toAbsolutePath
    println(s"Le répertoire actuel est : $currentPath")

    println("Saisir chemin racine:")
    val hdfsPath = scala.io.StdIn.readLine()
    println(s"chemin est : ${hdfsPath.toString}")

    //////////////////////////////////////////////
    //val path_flight = "FlightsLight"
    //val path_weather = "WeatherLight"
    val path_flight = "FlightsLight"
    val path_weather = "WeatherLight"
    val path_table_finale = "TableML"
    val dbfsDir_flight = hdfsPath + path_flight
    val dbfsDir_weather = hdfsPath + path_weather
    val dbfsDir_timezone = hdfsPath
    //val dbfsDir_parameters = saisie_clavier1
    val dbfsDir_table_finale = hdfsPath + path_table_finale
    //////////////////////////////////////////////////

    /*
    val dbfsDir_flight1 = "/Users/slimkachkachi/IdeaProjects/FlightsProjects/Flights"
    val dbfsDir_weather1 = "/Users/slimkachkachi/IdeaProjects/FlightsProjects/Weather"
    val dbfsDir_timezone1 = "/Users/slimkachkachi/IdeaProjects/FlightsProjects/wban_airport_timezone.csv"
    //val dbfsDir_parameters1 = Paths.get(path_projet).resolve("ParamsTransform.txt")
    val dbfsDir_table_finale1 = "/Users/slimkachkachi/IdeaProjects/FlightsProjects/TableML"
    */

    /*
    val dbfsDir_flight1 = "/students/iasd_20222023/skachkachi/ProjectFlight/FlightsLight"
    val dbfsDir_weather1 = "/students/iasd_20222023/skachkachi/ProjectFlight/WeatherLight"
    val dbfsDir_timezone1 = "/students/iasd_20222023/skachkachi/ProjectFlight/wban_airport_timezone.csv"
    //val dbfsDir_parameters1 = "/students/iasd_20222023/skachkachi/ProjectFlight/ParamsTransform.txt"
    val dbfsDir_table_finale1 = "/students/iasd_20222023/skachkachi/ProjectFlight/TableML"
    */

    println("chemin relatif Dir :", dbfsDir_flight)
    println("chemin relatif Dir :", dbfsDir_weather)
    println("chemin relatif Dir :", dbfsDir_timezone)
    //println("chemin relatif Dir :", dbfsDir_parameters)
    println("chemin relatif Dir :", dbfsDir_table_finale)

    /*def readParametersFromFile(filePath: String): Map[String, String] = {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()

      lines.map { line =>
        val parts = line.split("=")
        parts(0) -> parts(1)
      }.toMap
    }*/

    /*
    ////////////////////////////////////////////////
    val parameters = readParametersFromFile(dbfsDir_parameters+"ParamsTransform.txt")
    val param1Value = parameters("NombrePartitionsJointure").toInt
    println(s"nbez sz partitions demandées pour jointure : $param1Value")
    //////////////////////////////////////////
    */

    //********************************

    val init_flights_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dbfsDir_flight)

    //println("nombre d'enregistrements Flights :",init_flights_df.count())

    val init_weather_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dbfsDir_weather)

    //println("nombre d'enregistrements Weather :",init_weather_df.count())

    val timezone_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dbfsDir_timezone)

    //init_flights_df.show(false)
    //init_weather_df.show(false)
    //timezone_df.show(false)


    //val parameters = readParametersFromFile(dbfsDir_parameters1.toString)
    //val param1Value = parameters("NombrePartitionsJointure").toInt
    println("Saisir nbre de partitions pour jointure:")
    val saisie_clavier2 = scala.io.StdIn.readLine()
    val param1Value = saisie_clavier2.toInt
    print(s"nbre  text $param1Value")

    //println(s"nbez sz partitions demandées pour jointure : $param1Value")
    /////////////////////////////////////////////

    //Suppression lignes inutiles etc.
    val fligthsDF=init_flights_df
      .na.fill(0,Array("WEATHER_DELAY","NAS_DELAY","ARR_DELAY_NEW"))
      .na.drop(Seq("CRS_ELAPSED_TIME"))  //drop les lignes qd le valeurs de cette colonne à Null
      .filter($"DIVERTED" =!= 1 && $"CANCELLED" =!= 1)
      .drop("DIVERTED","CANCELLED","_C12")
      .dropDuplicates() //supprime d'éventuelles lignes en doublon(en pratique il n'y a pas)

    val fligthsDF_table=fligthsDF
      .join(timezone_df,fligthsDF("ORIGIN_AIRPORT_ID") === timezone_df("AirportID"),"inner")
      .withColumnRenamed("TimeZone","TZone_ORIGIN_AIRPORT_ID")
      .withColumnRenamed("WBAN","ORIGIN_WBAN") //WBAN gardé servira pour filtrer les observations météo
      .drop("AirportID")
      .as("fligthsDF_O")
      .join(timezone_df,col("fligthsDF_O.DEST_AIRPORT_ID") === timezone_df("AirportID"),"inner")
      .withColumnRenamed("TimeZone","TZone_DEST_AIRPORT_ID")
      .withColumnRenamed("WBAN","DEST_WBAN")
      .drop("AirportID")
      .withColumn("Dest-Orig", col("TZone_DEST_AIRPORT_ID") - col("fligthsDF_O.TZone_ORIGIN_AIRPORT_ID"))
      .drop("TZone_DEST_AIRPORT_ID","TZone_ORIGIN_AIRPORT_ID")

    println("etape 1 correspondance flights et wban OK")

    /*//table finale Méthode Slim

    val Table_flights = fligthsDF_table
      .withColumn("FL_DATE", date_format(col("FL_DATE"), "yyyy-MM-dd"))
      .drop("ORIGIN_WBAN", "DEST_WBAN")
      //Calcul Timestamp Scheduled Departure Time (heure locale)
      .withColumn("CDT_hh",($"CRS_DEP_TIME"/100).cast("Int"))  //partie heure de CDT scheduled departure time
      .withColumn("CDT_mm",($"CRS_DEP_TIME"%100).cast("Int"))  //partie minute de CDT scheduled departure time
      .withColumn("CRS_DEP_TIMESTAMP", concat($"FL_DATE".cast("string"),lit(" "),$"CDT_hh".cast("string"),lit(":"),$"CDT_mm".cast("string")).cast("timestamp"))  //  timestamps Scheduled Departure Time
      //Calcul timestamp Schedule Arrival Time (heure locale)
      //SCHEDULE ARRIVAL TIME (heure locale) = SCHEDULE DEPARTURE TIME (heure locale) + CRS_ELAPSED_TIME (durée prévi vol) + (Xd-Xa)
      .withColumn("CRT_jj", (($"CRS_ELAPSED_TIME"/60)/24).cast("Int")) //impact en jours durée prévi vol
      .withColumn("CRT_hh", (($"CRS_ELAPSED_TIME"/60)%24).cast("Int")) //impact en heures durée prévi vol
      .withColumn("CRT_mm", ($"CRS_ELAPSED_TIME"%60).cast("Int")) //impact en minutes durée prévi vol
      .withColumn("Dest_mm", (($"CRT_mm"+$"CDT_mm")%60).cast("Int")) //partie minute Schedule Arrival Time
      .withColumn("Dest_hh", ((($"CRT_mm"+$"CDT_mm")/60+$"CRT_hh"+$"CDT_hh"+$"Dest-Orig")%24).cast("Int"))
      .withColumn("Dest_jj", (($"CRT_hh"+$"CDT_hh"+$"Dest-Orig")/24+col("CRT_jj")).cast("Int"))
      .withColumn("New_A_DATE", date_add($"FL_DATE",$"Dest_jj")) //nouvelle date Schedule Arrival To
      .withColumn("SCHEDULE_ARRIVAL_TIMESTAMP", concat(col("New_A_DATE").cast("string"),lit(" "),col("DEST_hh").cast("string"),lit(":"),col("DEST_mm").cast("string")).cast("timestamp")) // Timpestamp Schedule Arrival Time
      .select("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","CRS_DEP_TIMESTAMP","SCHEDULE_ARRIVAL_TIMESTAMP","ARR_DELAY_NEW") //new_arr_delay concervé comme base de la class /sera utilisée en phase de ML
        */


    //table finale Méthode Sara (plus facile à manipuler) v2

    val minutesToSecondsUDF = udf((minutes: Double) => (minutes * 60).toLong)
    val hoursToSecondsUDF = udf((hours: Double) => (hours * 60 * 60).toLong)

    val Table_flights = fligthsDF_table
      .withColumn("FL_DATE", date_format(col("FL_DATE"), "yyyy-MM-dd"))
      .drop("ORIGIN_WBAN", "DEST_WBAN")
      //Calcul TIMESTAMP SCHEDULED DEPARTURE TIME(heure locale)
      .withColumn("CRS_DEP_TIME_string", from_unixtime(unix_timestamp(format_string("%04d", $"CRS_DEP_TIME"), "HHmm"), "HH:mm"))
      .withColumn("DATE_CRS_DEP_TIME_string",
        from_unixtime(
          unix_timestamp(concat($"FL_DATE".cast("string"), lit(" "), $"CRS_DEP_TIME_string"), "yyyy-MM-dd HH:mm")))
      .withColumn("CRS_DEP_TIMESTAMP", to_timestamp(col("DATE_CRS_DEP_TIME_string")))
      //SCHEDULE ARRIVAL TIME (heure locale) = SCHEDULE DEPARTURE TIME (heure locale) + CRS_ELAPSED_TIME (durée prévi vol) + (Xd-Xa)
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP_string",
        from_unixtime(
          unix_timestamp($"DATE_CRS_DEP_TIME_string") + minutesToSecondsUDF($"CRS_ELAPSED_TIME") + hoursToSecondsUDF($"Dest-Orig")))
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP", to_timestamp(col("SCHEDULED_ARRIVAL_TIMESTAMP_string")))
      .drop("CRS_DEP_TIME_string", "DATE_CRS_DEP_TIME_string", "SCHEDULED_ARRIVAL_TIMESTAMP_string")
      .select("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIMESTAMP", "SCHEDULED_ARRIVAL_TIMESTAMP", "ARR_DELAY_NEW")

    println("etape 2 preprocessing flights OK")
    //Table_flights.show(false)

    //constitution de la liste regroupant les WBAN uniques des aeroports de départ et d'arrivée
    //normalement pas de WBAN qui soit en destination sans être un départ :-) => union peut être retirée
    val dest = fligthsDF_table.select($"DEST_AIRPORT_ID", $"DEST_WBAN")
      .withColumnRenamed("DEST_AIRPORT_ID", "AIRPORT_ID")
      .withColumnRenamed("DEST_WBAN", "F_WBAN")
      .distinct

    val origin = fligthsDF_table.select($"ORIGIN_AIRPORT_ID", $"ORIGIN_WBAN")
      .withColumnRenamed("ORIGIN_AIRPORT_ID", "AIRPORT_ID")
      .withColumnRenamed("ORIGIN_WBAN", "F_WBAN")
      .distinct

    val all_airports = dest.union(origin).distinct

    println("etape 3 all_aiports collection nbre d'aiports : ", all_airports.distinct().count())


    //pour filtage des enregistrements dont le WBAN est présent dans la liste des airports avant la jointure
    //pour filtrer les colonnes inutiles avant la jointure (pour l'alléger)

    //création de la séquence de WBAN issus des aeroports
    val serie_wban=all_airports.select("F_WBAN").as[Int].collect().toSeq

    //selection des seules colonnes utiles
    //val weather_attribut_FWBAN = Array("WBAN","Date","Time","DryBulbCelsius","SkyCOndition","Visibility","WindDirection","WindSpeed","WeatherType","StationPressure").map(col)
    val weather_attribut_FWBAN = Array("WBAN","Date","Time","DryBulbCelsius","SkyCondition","Visibility","WeatherType", "WindSpeed","HourlyPrecip").map(col)

    //filtrage des WBAN weather sur la base de serie_wban
    val weatherDF_table_prepa=init_weather_df
      .select(weather_attribut_FWBAN: _*)
      .where($"WBAN".isin(serie_wban:_*))

    println("etape 4 filtrage weather avec wban airport")
    //weatherDF_table_prepa.show()

    /*
    //code intial SLim sans le remplacement des valeurs manquantes par les moyennes du jour

    //filtage des enregistrements dont le WBAN est présent dans la liste des airports
    //attention conserver les time et date qui serviront pour ne retenir qu'une mesure par heure

    //selection des seules colonnes utiles
    val weather_attribut = Array("AIRPORT_ID","Date","Time","DryBulbCelsius","SkyCOndition","Visibility","WindDirection","WindSpeed","WeatherType","StationPressure").map(col)


    val weatherDF_table=weatherDF_table_prepa
    .join(all_airports,weatherDF_table_prepa("WBAN") === all_airports("F_WBAN"),"inner") //retrait  WBAN sans aeroports
    .select(weather_attribut:_*)
    .withColumn("Time_hh",(col("Time")/100).cast("Int"))
    .withColumn("Time_mm",(col("Time")%100).cast("Int"))
    .withColumn("DryBulbCelsius",col("DryBulbCelsius").cast("double")).na.fill(0,Array("DryBulbCelsius"))
    .withColumn("SkyCOndition", when(col("SkyCOndition") === "M","").otherwise(col("SkyCOndition")))
    .withColumn("Visibility",col("Visibility").cast("double")).na.fill(0,Array("Visibility"))
    .withColumn("WindDirection", col("WindDirection").cast("int")).na.fill(0,Array("WindDirection"))
    .withColumn("WindSpeed",col("WindSpeed").cast("int")).na.fill(0,Array("WindSpeed"))
    .withColumn("StationPressure",col("StationPressure").cast("double")).na.fill(0,Array("StationPressure"))
    .withColumn("WeatherType", when(col("WeatherType") === "M","").otherwise(col("WeatherType")))
    .dropDuplicates("AIRPORT_ID","Date","Time_hh","Time_mm")  //supprimer doublons => important pour la table des - 12h

     */


    //Code à conserver avec compléments Arij

    val extractSkyCondition2: UserDefinedFunction = udf((inputString: String) => {
      Option(inputString) match {
        case Some(str) if str.nonEmpty =>
          val cloudLayers = str.trim.split("\\s+")
          val lastCloudLayer = cloudLayers.lastOption.getOrElse("OTHER")
          val pattern = "[A-Za-z]+(?=\\d*$)".r
          val result = pattern.findFirstIn(lastCloudLayer)
          result.getOrElse("OTHER")
        case _ => "M"
      }
    })

    //UDF pour extraire les conditions météo de WeatherType
    //pour l'instant non utilisée
    /*val weatherTypeTransformUDF: UserDefinedFunction = udf((ColumnName: String) => {
      val weatherTtypesList = List("RA", "SN", "FG", "FG+", "WIND", "FZDZ", "FZRA", "MIFG", "FZFG")
      val defaultValue = "OTHER"
      if (weatherTtypesList.contains(ColumnName)) {
        ColumnName
      }
      else {
        defaultValue
      }
    })*/

    //selection des seules colonnes utiles
    //val weather_attribut = Array("AIRPORT_ID", "Date", "Time", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindDirection", "WindSpeed", "WeatherType", "StationPressure").map(col)
    val weather_attribut = Array("AIRPORT_ID","Date","Time","DryBulbCelsius","SkyCOndition","Visibility","WindSpeed","WeatherType","HourlyPrecip").map(col)

    val weather = weatherDF_table_prepa
      .join(all_airports, weatherDF_table_prepa("WBAN") === all_airports("F_WBAN"), "inner")
      .select(weather_attribut: _*)
      .withColumn("Time_hh", (col("Time") / 100).cast("Int"))
      .withColumn("Time_mm", (col("Time") % 100).cast("Int"))
      .withColumn("DryBulbCelsius", col("DryBulbCelsius").cast("double"))
      //comme discuté on laisse les M, qui se retrouveront dans le randomforest...seront separables des null issus des autres attributs
      .withColumn("SkyCondition", extractSkyCondition2(col("SkyCondition")))
      .withColumn("Visibility", col("Visibility").cast("double"))
      //.withColumn("WindDirection", col("WindDirection").cast("int")).na.fill(0,Array("WindDirection"))
      .withColumn("WindSpeed", when(col("WindSpeed") === "VR", -1).otherwise(col("WindSpeed")).cast("int"))
      //comme discuté on laisse les M, qui se retrouveront dans le randomforest...seront separables des null issus des autres attributs
      .withColumn("WeatherType", when(col("WeatherType") === "M","").otherwise($"WeatherType"))
      .withColumn("HourlyPrecip",when(col("HourlyPrecip") === "T" || col("HourlyPrecip") === " ",0).otherwise(col("HourlyPrecip")).cast("double"))
       //.withColumn("StationPressure", col("StationPressure").cast("double"))
      //.withColumn("WeatherType", when(col("WeatherType") === "M", "").otherwise(col("WeatherType")))
      .dropDuplicates("AIRPORT_ID", "Date", "Time_hh", "Time_mm")



    val daily_average = weather
      //.filter($"WindSpeed" =!= -1 && !$"DryBulbCelsius".isNull && !$"WindSpeed".isNull && !$"StationPressure".isNull && !$"Visibility".isNull)
      .filter($"WindSpeed" =!= -1 && !$"DryBulbCelsius".isNull && !$"WindSpeed".isNull && !$"HourlyPrecip".isNull && !$"Visibility".isNull)
      .groupBy("Date","AIRPORT_ID")
      .agg(mean("DryBulbCelsius").as("average_temperature"),
      mean("WindSpeed").as("average_windspeed"),
      //mean("StationPressure").as("average_stationpressure"),
      mean("Visibility").as("average_visibility"),
      mean("HourlyPrecip").as("average_hourlyprecip"))
      .withColumnRenamed("AIRPORT_ID", "AVERAGE_AIRPORT_ID")
      .withColumnRenamed("Date", "AVERAGE_Date")

    println("etape 5 table weather avec les valeurs moyennes par airport et date")
    //daily_average.show()

    val weatherDF_table = weather
      .join(daily_average, weather("AIRPORT_ID") === daily_average("AVERAGE_AIRPORT_ID") && weather("Date") === daily_average("AVERAGE_Date"), "inner")
      .drop("AVERAGE_AIRPORT_ID", "AVERAGE_Date")
      .withColumn("DryBulbCelsius", when($"DryBulbCelsius".isNull, daily_average("average_temperature")).otherwise($"DryBulbCelsius"))
      .withColumn("Visibility", when($"Visibility".isNull, daily_average("average_visibility")).otherwise($"Visibility"))
      .withColumn("WindSpeed", when($"WindSpeed".isNull, daily_average("average_windspeed")).otherwise($"WindSpeed"))
      //.withColumn("StationPressure", when($"StationPressure".isNull, daily_average("average_stationpressure")).otherwise($"StationPressure"))
      .withColumn("HourlyPrecip", when($"HourlyPrecip".isNull, daily_average("average_hourlyprecip")).otherwise($"HourlyPrecip"))

    println("etape 6 table weather avec les moyennes à la place des valeurs initiales null")
    weatherDF_table.show()

    //etape 1 : table ne contenant qu'une mesure par heure
    //posture adoptée: prise de la premiere dans la série temporelle après les avoir classées par ordre croissant

    //passage à l'étape suivante création de la table qui servira à la jointure avec weahterDF_table
    // afin de ne garder qu'une mesure par heure
    val weatherDF_table_G = weatherDF_table
      .orderBy($"AIRPORT_ID", $"Date", $"Time_hh", $"Time_mm".asc)
      .groupBy("AIRPORT_ID", "Date", "Time_hh")
      .agg(collect_list("Time_mm").as("collection"))
      .withColumn("Time_mm", col("collection").getItem(0).cast("Int"))
      .drop("collection")
      .withColumnRenamed("AIRPORT_ID", "W_AIRPORT_ID")
      .withColumnRenamed("Date", "W_Date")
      .withColumnRenamed("Time_hh", "W_Time_hh")
      .withColumnRenamed("Time_mm", "W_Time_mm")

    println("etape 7 table  weather avec une seule mesure par heure")
    //weatherDF_table_G.show()

    //étape 2 : jointure avec la table weather contenant que les bonnes colonnes
    //attention à ce stade il n'y a pas encore de timestamp
    //option 1 on garde les valeurs des minutes   //option 2 on ecrase les minutes par 00

    //val columns = Array("AIRPORT_ID", "Weather_TIMESTAMP", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindDirection", "WindSpeed", "WeatherType", "StationPressure")
    //  .map(col)

    val columns = Array("AIRPORT_ID", "Weather_TIMESTAMP", "DryBulbCelsius", "SkyCOndition", "Visibility", "WindSpeed", "WeatherType", "HourlyPrecip")
      .map(col)

    val table_weather = weatherDF_table.join(weatherDF_table_G, weatherDF_table("AIRPORT_ID") === weatherDF_table_G("W_AIRPORT_ID") &&
      weatherDF_table("Date") === weatherDF_table_G("W_Date") &&
      weatherDF_table("Time_hh") === weatherDF_table_G("W_Time_hh") &&
      weatherDF_table("Time_mm") === weatherDF_table_G("W_Time_mm")
      , "inner")
      .drop("W_AIRPORT_ID", "W_Date", "W_Time_hh", "W_Time_mm")
      .withColumn("Date", to_date($"Date".cast("string"), "yyyyMMdd"))
      .withColumn("Weather_TIMESTAMP", concat(col("Date").cast("string"), lit(" "), col("Time_hh").cast("string"), lit(":"), lit(0)).cast("timestamp"))
      .drop("Date", "Time", "Time_hh", "Time_mm")
      .select(columns: _*)

    println("etape 8 table weather finale avec les bons attributs")

    //création des dataset
    val table_flight_DS: Dataset[Flights] = Table_flights
      .as[Flights]

    val table_weather_DS: Dataset[Weather] = table_weather
      .as[Weather]

    // Get the current timestamp
    val currentTime_debut: Long = System.currentTimeMillis() / 1000
    println(currentTime_debut)

    /*1 iere étape production de FT sous le format de((join_key, table_tag), value) où join_key = (airport_id, date)
    ATTENTION = appliquer la consigne de l
    'article: si Date +
    12 h = Date + 1 jour alors emit (aiport_id, date + 1 j) + emit(aiport_id, date)
    NB = ARR_DELAY_NEW laissé avec FT (servira en input du ML avec seuil paramétrage de retard)*/

    //création de FT_Origin ((join_key),"FT") avec join_key = (airport_id_origin,date)

    val FT_Origin = table_flight_DS
      .orderBy($"ORIGIN_AIRPORT_ID", to_date($"CRS_DEP_TIMESTAMP"))
      //.orderBy($"ORIGIN_AIRPORT_ID",$"Date")
      .flatMap { row_flight =>

        // Convert java.sql.Timestamp to java.sql.Date
        val Date_CRS_Origin: Date = new Date(row_flight.CRS_DEP_TIMESTAMP.getTime)  //version originelle

        val joinkey = (row_flight.ORIGIN_AIRPORT_ID, Date_CRS_Origin) //version originelle

        val flight_row = (row_flight.ORIGIN_AIRPORT_ID,
          row_flight.DEST_AIRPORT_ID,
          row_flight.CRS_DEP_TIMESTAMP,
          //row_flight.ACTUAL_DEPARTURE_TIMESTAMP,
          row_flight.SCHEDULED_ARRIVAL_TIMESTAMP,
          //row_flight.ACTUAL_ARRIVAL_TIMESTAMP,
          row_flight.ARR_DELAY_NEW)

        Seq(((joinkey, "FT"), flight_row))
      }
      .withColumnRenamed("_1", "FT_KEY")
      .withColumnRenamed("_2", "Flights_List")

    println("etape 09")
    //FT_Origin.show(false)

    //Nouvelle VERSION avec si date+12h => ajout de date+1J pour avoir une plage de 36h (de 12:00 à j-1 à 24:00 à j)
    //création de OT ((join_key),"OT") avec join_key = (airport_id,date)
    //sur cet exemple 704 lignes qui n'ont pa 36 mesures par jour

    val OT = table_weather_DS
      .orderBy($"AIRPORT_ID", $"Weather_TIMESTAMP".desc) //pas sûr que cela soit utile
      .flatMap {  row_weather =>
        // Convert java.sql.Timestamp to java.sql.Date
        val Date_Weather: Date = new Date(row_weather.Weather_TIMESTAMP.getTime)
        val joinkey = (row_weather.AIRPORT_ID, Date_Weather)
        val weather_row = (
          row_weather.AIRPORT_ID,
          row_weather.Weather_TIMESTAMP,
          row_weather.DryBulbCelsius,
          row_weather.SkyCOndition,
          row_weather.Visibility,
          //row_weather.WindDirection,
          row_weather.WindSpeed,
          row_weather.WeatherType,
          //row_weather.StationPressure)
          row_weather.HourlyPrecip)

        // etape 1: convertir le java.sql.timestamp en java.time.LocalDateTime
        val localDateTime = row_weather.Weather_TIMESTAMP.toLocalDateTime
        //ajout de 12 heures
        val localDateTimePlus12heures = localDateTime.plus(12, ChronoUnit.HOURS)
        // Convertir le résultat java.time.LocalDateTime en java.sql.Timestamp
        val date_localDateTimePlus12heures = localDateTimePlus12heures.toLocalDate
        //ajout de 1 jour
        val localDateTimePlus1jour = localDateTime.plus(1, ChronoUnit.DAYS)
        // Convertir le résultat java.time.LocalDateTime en java.sql.Timestamp
        val date_localDateTimePlus1jour = localDateTimePlus1jour.toLocalDate

        if (date_localDateTimePlus12heures == date_localDateTimePlus1jour) {
          Seq(((joinkey, "OT"), weather_row), ((row_weather.AIRPORT_ID, Date.valueOf(date_localDateTimePlus1jour)), "OT") -> weather_row)
        } else {
          Seq(((joinkey, "OT"), weather_row))
        }
      }
      .withColumnRenamed("_1", "OT_KEY")
      .withColumnRenamed("_2", "OT_VALUE")
      .groupBy("OT_KEY")
      .agg(collect_list("OT_VALUE").as("Weather_List"))
      .withColumn("nbre",functions.size($"Weather_List")) //pour préparer la suppression des jours où il y a moins de 24 mesures (une/heure)
      .filter($"nbre" === 36)
      .drop("nbre")

    println("etape 10 weather origin avec 36 heures Ok")

    //OT.printSchema()
    //OT.show(false)

    /*2 ieme étape: haspartitionning pour colocalisation (et copartitionnement) choix d 'une partition de 4*/


    val NumPartitions = param1Value
    println("rappel nbre partitions pour jointure")
    val OT_partition = OT.repartition(NumPartitions, $"OT_KEY".getItem("_1")).persist
    val FT_Origin_partition = FT_Origin.repartition(NumPartitions, $"FT_KEY".getItem("_1"))

    println(s"etape 11 de partitioning OK avec nbrede partitions = $NumPartitions" )
    //OT_partition.select("OT_KEY").printSchema()
    //OT_partition.select("OT_KEY").show(false)

    //FT_Origin_partition.select("FT_KEY").printSchema()
    //FT_Origin_partition.select("FT_KEY").show(false)


    //3ieme étape : jointure

    val FOT_joined_Origin= OT_partition
      .join(FT_Origin_partition,OT_partition("OT_KEY").getItem("_1") === FT_Origin_partition("FT_KEY").getItem("_1"),"inner")

    println(s"etape 12 de jointure OK FT avec OT Origin")
    //FOT_joined_Origin.printSchema()
    //FOT_joined_Origin.show(false)

    //4ieme étape : aggrégation pour obtenir (FT,WO,Class)

    val FOT_Etape_2 = FOT_joined_Origin
      .map { raw =>

        def rowToWeather(row: Row): Weather = {
          Weather(
            AIRPORT_ID = row.getInt(0),
            Weather_TIMESTAMP = row.getTimestamp(1),
            DryBulbCelsius = row.getDouble(2),
            SkyCOndition = row.getString(3),
            Visibility = row.getDouble(4),
            //WindDirection = row.getInt(5),
            //WindSpeed = row.getDouble(6),
            WindSpeed = row.getDouble(5),
            //WeatherType = row.getString(7),
            WeatherType = row.getString(6),
            //StationPressure = row.getDouble(8)
            HourlyPrecip = row.getDouble(7)
          )
        }

        val weather_Array = raw.getAs[Seq[Row]](1)
        val flights = Flights(
          ORIGIN_AIRPORT_ID = raw.getStruct(3).getInt(0),
          DEST_AIRPORT_ID = raw.getStruct(3).getInt(1),
          CRS_DEP_TIMESTAMP = raw.getStruct(3).getTimestamp(2),
          //ACTUAL_DEPARTURE_TIMESTAMP = raw.getStruct(3).getTimestamp(3),
          //SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(4),
          SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(3),
          //ACTUAL_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(5),
          //ARR_DELAY_NEW = raw.getStruct(3).getDouble(6)
          ARR_DELAY_NEW = raw.getStruct(3).getDouble(4)
        )


        // etape 1: convertir le java.sql.timestamp en java.time.LocalDateTime
        val timestamp_CRS_Departure = flights.CRS_DEP_TIMESTAMP.toLocalDateTime
        //Etapes 2 & 3 : retirer 12h et convertir le résultat de java.time.LocalDateTime en java.sql.Timestamp
        val timestamp_CRS_Departure_moins12h: Timestamp = Timestamp.valueOf(timestamp_CRS_Departure.plus(-12, ChronoUnit.HOURS))

        val sortedWeatherArray: Array[Weather] = weather_Array.sortWith { (row1, row2) => row1.getAs[java.sql.Timestamp]("_2").before(row2.getAs[java.sql.Timestamp]("_2"))
        }.map(rowToWeather(_)).toArray

        val AT = ArrayBuffer.empty[Weather]
        AT.clear()

        sortedWeatherArray.foreach { row => {
          //val localDateTime : Timestamp= Timestamp.valueOf(row.Weather_TIMESTAMP.toLocalDateTime())
          val localDateTime = row.Weather_TIMESTAMP
          //if (timestamp_CRS_Departure_moins12h.before(localDateTime) && localDateTime.before(flights.CRS_DEP_TIMESTAMP)) AT += row
          if (timestamp_CRS_Departure_moins12h.before(localDateTime) &&
            (localDateTime.before(flights.CRS_DEP_TIMESTAMP) || localDateTime.equals(flights.CRS_DEP_TIMESTAMP))
          )
            AT += row
        }
        }

        (flights, AT)

      }
      .withColumnRenamed("_1", "FT")
      .withColumnRenamed("_2", "WO_List")
    //.withColumn("nbre",size($"WO_List"))

    println(" Etape 13 de FT avec les 12 heures WO précédents le OK")
    //FOT_Etape_2.show()

    //Etape création de ((join_key,'FT"),(FT,WD)) en vue de la jointure avec OT

    val FT_Destination = FOT_Etape_2
      .flatMap { raw =>

        def rowToWeather(row: Row): Weather = {
          Weather(
            AIRPORT_ID = row.getInt(0),
            Weather_TIMESTAMP = row.getTimestamp(1),
            DryBulbCelsius = row.getDouble(2),
            SkyCOndition = row.getString(3),
            Visibility = row.getDouble(4),
            //WindDirection = row.getInt(5),
            //WindSpeed = row.getDouble(6),
            WindSpeed = row.getDouble(5),
            //WeatherType = row.getString(7),
            WeatherType = row.getString(6),
            //StationPressure = row.getDouble(8)
            HourlyPrecip=row.getDouble(7)
          )
        }

        val weather_Array = raw.getAs[Seq[Row]](1).map(rowToWeather(_)).toArray

        val flights = Flights(
          ORIGIN_AIRPORT_ID = raw.getStruct(0).getInt(0),
          DEST_AIRPORT_ID = raw.getStruct(0).getInt(1),
          CRS_DEP_TIMESTAMP = raw.getStruct(0).getTimestamp(2),
          //ACTUAL_DEPARTURE_TIMESTAMP = raw.getStruct(0).getTimestamp(3),
          //SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(0).getTimestamp(4),
          SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(0).getTimestamp(3),
          //ACTUAL_ARRIVAL_TIMESTAMP = raw.getStruct(0).getTimestamp(5),
          //ARR_DELAY_NEW = raw.getStruct(0).getDouble(6)
          ARR_DELAY_NEW = raw.getStruct(0).getDouble(4)
        )


        // Convert java.sql.Timestamp to java.sql.Date
        val Date_CRS_Destination: Date = new Date(flights.SCHEDULED_ARRIVAL_TIMESTAMP.getTime)

        val joinkey = (flights.DEST_AIRPORT_ID, Date_CRS_Destination)

        Seq(((joinkey, "FT"), flights, weather_Array))
      }
      .withColumnRenamed("_1", "FT_KEY")
      .withColumnRenamed("_2", "Flight_List")
      .withColumnRenamed("_3", "Weather_List_Origin")

    println("etape 14: FT_Destination FT,FT_list, WO OK en vue 2ieme jointure")
    //FT_Destination.show()

    //haspartitionning pour colocalisation (et copartitionnement) choix d'une partition de 4
    val FT_Destination_partition=FT_Destination.repartition(NumPartitions,$"FT_KEY".getItem("_1"))
    println(s"2ieme partie  partitioning OK avec nbrede partitions = $param1Value")

    //jointure avec OT (destination)
    val FOT_joined_final = OT_partition.join(FT_Destination_partition, OT_partition("OT_KEY")
      .getItem("_1") === FT_Destination_partition("FT_KEY").getItem("_1"), "inner")
    println(s"2ieme partie  de jointure OK ")

    //FOT_joined_final.show()
    // aggrégation pour obtenir (FT,WO,WD,Class)
    //code OK

    val FOT_final = FOT_joined_final
      .map { raw =>

        def rowToWeather(row: Row): Weather = {
          Weather(
            AIRPORT_ID = row.getInt(0),
            Weather_TIMESTAMP = row.getTimestamp(1),
            DryBulbCelsius = row.getDouble(2),
            SkyCOndition = row.getString(3),
            Visibility = row.getDouble(4),
            //WindDirection = row.getInt(5),
            //WindSpeed = row.getDouble(6),
            WindSpeed = row.getDouble(5),
            //WeatherType = row.getString(7),
            WeatherType = row.getString(6),
            //StationPressure = row.getDouble(8)
            HourlyPrecip = row.getDouble(7)
          )
        }

        val weather_D_Array = raw.getAs[Seq[Row]](1)
        val flights = Flights(
          ORIGIN_AIRPORT_ID = raw.getStruct(3).getInt(0),
          DEST_AIRPORT_ID = raw.getStruct(3).getInt(1),
          CRS_DEP_TIMESTAMP = raw.getStruct(3).getTimestamp(2),
          //ACTUAL_DEPARTURE_TIMESTAMP = raw.getStruct(3).getTimestamp(3),
          //SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(4),
          SCHEDULED_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(3),
          //ACTUAL_ARRIVAL_TIMESTAMP = raw.getStruct(3).getTimestamp(5),
          //ARR_DELAY_NEW = raw.getStruct(3).getDouble(6)
          ARR_DELAY_NEW = raw.getStruct(3).getDouble(4)
        )


        val weather_O_Array = raw.getAs[Seq[Row]](4).map(rowToWeather(_)).toArray


        // etape 1: convertir le java.sql.timestamp en java.time.LocalDateTime
        val timestamp_CRS_Arrival = flights.SCHEDULED_ARRIVAL_TIMESTAMP.toLocalDateTime
        //Etapes 2 & 3 : retirer 12h et convertir le résultat de java.time.LocalDateTime en java.sql.Timestamp
        val timestamp_CRS_Arrival_moins12h: Timestamp = Timestamp.valueOf(timestamp_CRS_Arrival.plus(-12, ChronoUnit.HOURS))


        val sortedWeatherArray_D: Array[Weather] = weather_D_Array.sortWith { (row1, row2) => row1.getAs[java.sql.Timestamp]("_2").before(row2.getAs[java.sql.Timestamp]("_2"))
        }.map(rowToWeather(_)).toArray



        val AT = ArrayBuffer.empty[Weather]
        AT.clear()


        sortedWeatherArray_D.foreach { row => {
            val localDateTime = row.Weather_TIMESTAMP
            if (timestamp_CRS_Arrival_moins12h.before(localDateTime) &&
              (localDateTime.before(flights.SCHEDULED_ARRIVAL_TIMESTAMP) || localDateTime.equals(flights.SCHEDULED_ARRIVAL_TIMESTAMP) )
              )
          AT += row
        }
        }

        (flights, weather_O_Array, AT)
      }
      .withColumnRenamed("_1", "FT")
      .withColumnRenamed("_2", "WO_List")
      .withColumnRenamed("_3", "WD_List")
    //.withColumn("Nbre_O",size($"WO_List"))
    //.withColumn("Nbre_D",size($"WD_List"))

    println("etape 15: FOT_joined_final FT,WO,WO OK")
    FOT_final.show()
    val currentTime_fin: Long = System.currentTimeMillis() / 1000
    val duree = currentTime_fin - currentTime_debut
    println("durée jointure :", duree)



    //remise des données en colonnes : un n-uplet Wx par colonne


    val table_FOT_final = FOT_final
      .withColumn("WO_11H", $"WO_List".getItem(0))
      .withColumn("WO_10H", $"WO_List".getItem(1))
      .withColumn("WO_09H", $"WO_List".getItem(2))
      .withColumn("WO_08H", $"WO_List".getItem(3))
      .withColumn("WO_07H", $"WO_List".getItem(4))
      .withColumn("WO_06H", $"WO_List".getItem(5))
      .withColumn("WO_05H", $"WO_List".getItem(6))
      .withColumn("WO_04H", $"WO_List".getItem(7))
      .withColumn("WO_03H", $"WO_List".getItem(8))
      .withColumn("WO_02H", $"WO_List".getItem(9))
      .withColumn("WO_01H", $"WO_List".getItem(10))
      .withColumn("WO_00H", $"WO_List".getItem(11))
      .withColumn("ORIGIN_AIRPORT_ID", $"FT".getItem("ORIGIN_AIRPORT_ID"))
      .withColumn("DEST_AIRPORT_ID", $"FT".getItem("DEST_AIRPORT_ID"))
      .withColumn("CRS_DEP_TIMESTAMP", $"FT".getItem("CRS_DEP_TIMESTAMP"))
      //.withColumn("ACTUAL_DEPARTURE_TIMESTAMP", $"FT".getItem("ACTUAL_DEPARTURE_TIMESTAMP"))
      .withColumn("SCHEDULED_ARRIVAL_TIMESTAMP", $"FT".getItem("SCHEDULED_ARRIVAL_TIMESTAMP"))
      //.withColumn("ACTUAL_ARRIVAL_TIMESTAMP", $"FT".getItem("ACTUAL_ARRIVAL_TIMESTAMP"))
      .withColumn("WD_11H", $"WD_List".getItem(0))
      .withColumn("WD_10H", $"WD_List".getItem(1))
      .withColumn("WD_09H", $"WD_List".getItem(2))
      .withColumn("WD_08H", $"WD_List".getItem(3))
      .withColumn("WD_07H", $"WD_List".getItem(4))
      .withColumn("WD_06H", $"WD_List".getItem(5))
      .withColumn("WD_05H", $"WD_List".getItem(6))
      .withColumn("WD_04H", $"WD_List".getItem(7))
      .withColumn("WD_03H", $"WD_List".getItem(8))
      .withColumn("WD_02H", $"WD_List".getItem(9))
      .withColumn("WD_01H", $"WD_List".getItem(10))
      .withColumn("WD_00H", $"WD_List".getItem(11))
      .withColumn("Class", $"FT".getItem("ARR_DELAY_NEW"))
      .drop("FT", "WO_List", "WD_List")

    println("etape split n_uplets Wx par colonne : OK")
    //table_FOT_final.show(false)

    val fractions = Array(0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05,0.05)
    val splits = table_FOT_final.randomSplit(fractions)

    splits.zipWithIndex.foreach { case (splits, i) =>
      splits.write.format("parquet")
        .option("header", "true")
        .mode("overwrite")
        .save(dbfsDir_table_finale + "/chunk_" + i + ".parquet")
    }

    println("etape 16 ecriture OK")
  }
}

