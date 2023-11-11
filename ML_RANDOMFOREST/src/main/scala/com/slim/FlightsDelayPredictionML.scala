package com.slim

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex

object FlightsDelayPredictionML {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(s"FlightsDelayPredictionML")
      //.master("spark://localhost:7077")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")


    println("Saisir chemin d'accès à la racine du projet :")
    val hdfsPath = scala.io.StdIn.readLine()
    val path_DataML = "TableML"
    val dbfsDir_ML  =hdfsPath + path_DataML

    //val dbfsDir_ML = "/Users/slimkachkachi/IdeaProjects/FlightsProjectsML/TableML"
    //val dbfsDir_ML = "/students/iasd_20222023/skachkachi/ProjectFlight/TableML"
    println(s"chemin d'accès à la table : $dbfsDir_ML")


    /*
    //code pour récupérer tous les paramètres dans un fichier texte
    def readParametersFromFile(filePath: String): Map[String, String] = {
      val source = Source.fromFile(filePath)
      val lines = source.getLines().toList
      source.close()

      lines.map { line =>
        val parts = line.split("=")
        parts(0) -> parts(1)
      }.toMap
    }
    */
    //val path_Paramters = "ParamsML.txt"
    //val dbfsDir_parameters = path_projet + path_Paramters

    //val parameters = readParametersFromFile(dbfsDir_parameters)
    //val PlageHoraireOrigin = parameters("PlageHoraireOrigin").toInt
    //val PlageHoraireDestination = parameters("PlageHoraireDestination").toInt
    //val ThresdholdDelay = parameters("Thresdhold_late").toInt


    val annee = 0
    val mois = 0
    val jour = 0

    println("Saisir plage horaire avant CRS:")
    val saisie_clavier1 = scala.io.StdIn.readLine()
    val PlageHoraireOrigin = saisie_clavier1.toInt

    println("Saisir plage horaire après  Scheduled Arrival :")
    val saisie_clavier2 = scala.io.StdIn.readLine()
    val PlageHoraireDestination = saisie_clavier2.toInt

    println("Saisir seuil de retard en mn :")
    val saisie_clavier3 = scala.io.StdIn.readLine()
    val ThresdholdDelay = saisie_clavier3.toInt

    println("Saisir numtree pour RandomForest :")
    val saisie_clavier5 = scala.io.StdIn.readLine()
    val SaisieNumTree = saisie_clavier5.toInt

    println(s"plage horaire origin prise en compte : $PlageHoraireOrigin")
    println(s"plage horaire destination prise en compte : $PlageHoraireDestination")
    println(s"seuil de retard : $ThresdholdDelay")
    println(s"num Tree pour RandomForest : $SaisieNumTree")


    //récupération des données issues de la phase de preprocessing et transformation
    val chunks = (0 until 20).map { i =>
      spark.read.format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(dbfsDir_ML + "/chunk_" + i + ".parquet")
    }

    //creation of target depending on thresdhold, year, month and date columns, cast airport_id to string,
    val table = chunks.reduce(_ union _)
      .withColumn("Target", when($"Class" >= ThresdholdDelay, 1).otherwise(0))
      .withColumn("ORIGIN_AIRPORT_ID", $"ORIGIN_AIRPORT_ID".cast("string"))
      .withColumn("DEST_AIRPORT_ID", $"DEST_AIRPORT_ID".cast("string"))
      .withColumn("Hour_SO", date_format($"CRS_DEP_TIMESTAMP", "HH").cast("int"))
      .withColumn("Hour_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "HH").cast("int"))
      .withColumn("Day_SO", date_format($"CRS_DEP_TIMESTAMP", "F").cast("int"))
      .withColumn("Day_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "F").cast("int"))
      .withColumn("Month_SO", date_format($"CRS_DEP_TIMESTAMP", "M").cast("int"))
      .withColumn("Month_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "M").cast("int"))
      .withColumn("Year_SO", date_format($"CRS_DEP_TIMESTAMP", "y").cast("int"))
      .withColumn("Year_SA", date_format($"SCHEDULED_ARRIVAL_TIMESTAMP", "y").cast("int"))
      .drop("Class", "CRS_DEP_TIMESTAMP", "SCHEDULED_ARRIVAL_TIMESTAMP")

    println("etape 1 chargement données OK")
    //table.show(false)

    //Array of flight's attributs
    val col_constante = Array("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","Hour_SO","Hour_SA","Month_SO","Month_SA","Day_SO","Day_SA","Year_SO","Year_SA","Target")
    //split of weather tuples
    val numberPattern: Regex = "\\d+".r

    val col_plage_horaire_origin = table.columns
      .filter(colName => colName.contains("WO"))
      .flatMap { colName =>
        numberPattern.findAllIn(colName).toList match {
          case Nil => None
          case numbers => {
            val nbre = numbers.map(_.toInt)
            if (nbre.exists(_ <= PlageHoraireOrigin)) Some(colName) else None
          }
        }
      }


    val col_plage_horaire_destination = table.columns
      .filter(colName => colName.contains("WD"))
      .flatMap { colName =>
        numberPattern.findAllIn(colName).toList match {
          case Nil => None
          case numbers => {
            val nbre = numbers.map(_.toInt)
            if (nbre.exists(_ <= PlageHoraireDestination)) Some(colName) else None
          }
        }
      }


    //liste des colonnes finalement retenues pour le ML

    //val table_reduite = table.select((col_plage_horaire_origin ++ col_plage_horaire_destination ++ col_constante).map(col): _*)
    val table_reduite = table.select((col_constante).map(col): _*)

    println("etape 2, table avec les plages horaires sélectionnées")
    table_reduite.show(false)

    //pour splitter automatiquement ttes les colonnes "struct"

    // Define a map of index to field names //not mandatory but help to match indices to attribut's names
    val indexToFieldMap: Map[Int, String] = Map(
      0 -> "AIRPORT_ID",
      1 -> "Weather_TIMESTAMP",
      2 -> "DryBulbCelsius",
      3 -> "SkyCOndition",
      4 -> "Visibility",
      5 -> "WindSpeed",
      6 -> "WeatherType",
      7 -> "HourlyPrecip"
      //5 -> "WindDirection",
      //6 -> "WindSpeed",
      //7 -> "WeatherType",
      //8 -> "StationPressure"
    )

    //les colonnes a splitter commencent ttes par W
    val columnsToApplySplit = table_reduite.columns.filter(colName => colName.contains("W")).toSeq
    //Une fois splittées, il faudra supprimer les anciennes colonnes
    val columnsToDrop = table_reduite.columns.filter(colName => colName.contains("W")).toList
    //longueur des struct
    val structSize = 9 // Assuming each struct has 3 components

    //split of each tuple to get one column per value in each tuple
    val raw_data_prep1 = columnsToApplySplit.foldLeft(table_reduite) { (tempDs, colName) =>
      val colNameIndices = (0 until structSize - 1).map(index => (index, s"${index}_$colName"))

      val updatedInnerDs = colNameIndices.foldLeft(tempDs) { (innerTempDs, indexAndNewColumnName) =>
        val (index, newColumnName) = indexAndNewColumnName
        val fieldName = indexToFieldMap(index) // Get the field name from the map
        val colExpr = col(s"$colName.$fieldName") as newColumnName
        //colExpr will be interpreted  ans executed as 'col("WD_00H.Weather_TIMESTAMP") as "1_WD_00H"'
        innerTempDs.withColumn(newColumnName, colExpr)
      }

      updatedInnerDs
    }
      .drop(columnsToDrop: _*)

    println("étape 3 : split des n-uplets weather")
    raw_data_prep1.show()

    //delete of airport_id columns for weather columns
    val raw_data_prep2 = raw_data_prep1
      .select(raw_data_prep1.columns.filter(colName => !(colName.contains("0_") )).map(col): _*)

    println("etape 4 : retrait des airports id en doublon")
    //raw_data_prep2.show()

    //récupération des noms des colonnes de type timestamp
    val TimeStampCol = raw_data_prep2.dtypes
      .filter(_._2 == "TimestampType")
      .map(_._1)
    //création des colonnes (year, mois,) jour et heure
    val raw_data_prep3 = TimeStampCol.toSeq.foldLeft(raw_data_prep2) { (tempDs, colName) =>
      val colName_Year = year(col(colName))
      val colName_Month = month(col(colName))
      val colName_JJ = dayofweek(col(colName))
      val colName_HH = hour(col(colName))
      tempDs
        .withColumn(s"Year_$colName", colName_Year)
        .withColumn(s"Month_$colName", colName_Month)
        .withColumn(s"Day_$colName", colName_JJ)
        .withColumn(s"Hour_$colName", colName_HH)
      }.drop(TimeStampCol: _*)

    //les colonnes 6_W a splitter en autant de colonne que de valeurs retenues
    val columnsWeatherTypeToApplySplit = raw_data_prep3.columns.filter(colName => colName.contains("6_W")).toSeq

    // Split columns and create new DataFrame
    val splitColumnsDf = raw_data_prep3.select(columnsWeatherTypeToApplySplit.map(col): _*)

    def createWeatherTypeColumns(weatherDf: DataFrame, weatherTypes: List[String]): DataFrame = {
      // Select columns to split based on column names starting with "6_W"
      val columnsWeatherTypeToApplySplit = weatherDf.columns.filter(colName => colName.startsWith("6_W")).toSeq

      // Initialize the DataFrame
      var df = weatherDf

      // Add new columns for each weather type
      for (colName <- columnsWeatherTypeToApplySplit) {
        for (weatherType <- weatherTypes) {
          df = df.withColumn(s"${colName}_${weatherType}", when(col(colName).contains(weatherType), 1).otherwise(0))
        }
      }

      df.drop(columnsWeatherTypeToApplySplit: _*)
    }

    val weatherTypes = List("RA","SN","FG","FG+","WIND","FZDZ","FZRA","MIFG","FZFG")

    val raw_data_prep4=createWeatherTypeColumns(raw_data_prep3,weatherTypes)
    raw_data_prep4.show()

    //shunt de la transformation de weathertype
    //val raw_data_prep4=raw_data_prep3
    println("étape 5 split des timestamp /on ne garde que les heures")

    //contrôle à supprimer
    raw_data_prep4.select(raw_data_prep4.columns.filter(colName => colName.contains("6_W") ).map(col): _*).show(false)

    //selection des colonnes year, month or day, depending on user choises
    val col_annee= raw_data_prep4.columns
      .filter(colName => colName.contains("Year"))

    val col_mois= raw_data_prep4.columns
      .filter(colName => colName.contains("Month"))

    val col_jour= raw_data_prep4.columns
      .filter(colName => colName.contains("Day"))

    val col_sans_date = ArrayBuffer[String]()

    for (col <- raw_data_prep4.columns) {
      if (annee == 0) {
        if (mois == 0) {
          if (jour == 0) {
            // Remove all columns that contain "Year", "Month", or "Day".
            if (!col.contains("Year") && !col.contains("Month") && !col.contains("Day")) {
              col_sans_date += col
            }
          } else {
            // Remove all columns that contain "Year" or "Month".
            if (!col.contains("Year") && !col.contains("Month")) {
              col_sans_date += col
            }
          }
        } else {
          if (jour == 0) {
            // Remove all columns that contain "Year" or "Day".
            if (!col.contains("Year") && !col.contains("Day")) {
              col_sans_date += col
            }
          } else {
            // Remove all columns that contain "Year".
            if (!col.contains("Year")) {
              col_sans_date += col
            }
          }
        }
      } else {
        if (mois == 0) {
          if (jour == 0) {
            // Remove all columns that contain "Month" or "Day".
            if (!col.contains("Month") && !col.contains("Day")) {
              col_sans_date += col
            }
          } else {
            // Remove all columns that contain "Month".
            if (!col.contains("Month")) {
              col_sans_date += col
            }
          }
        } else {
          if (jour == 0) {
            // Remove all columns that contain "Day".
            if (!col.contains("Day")) {
              col_sans_date += col
            }
          } else {
            // No columns need to be removed.
          }
        }
      }
    }

    //final table ready to be injected into the ML pipeline
    val raw_data = raw_data_prep4.select((col_sans_date).map(col):_*)
    println(("etape 6 selection colonnes selon choix année, mois..."))
    raw_data.show()


    //preparation pipeline de transformation data
    //extraction des noms des colonnes selon leur type dans le fichier final

    val NumCol = raw_data.dtypes
      .filter(tuple => tuple._2.equals("DoubleType") || tuple._2.equals("IntegerType"))
      .map(_._1).filterNot(_.contains("Target"))
    //NumCol.foreach(x => println(x))

    val StringCol = raw_data.dtypes
      .filter(_._2 == "StringType")
      .map(_._1)
    //StringCol.foreach(x => println(x))

    //variables pour pipeline
    val _label = "label"
    val _prefix = "indexed_"
    val _featuresVec = "featuresVec"
    val _featuresVecIndex = "features"

    var _featureIndices = Array(("", "")) //sera utile pour retrouver les noms des features sur la base des indices

    def AutoPipelineReg(textCols: Array[String], numericCols: Array[String], maxCat: Int, handleInvalid: String): Pipeline = {
      //StringIndexer
      val inAttsNames = textCols
      val outAttsNames = inAttsNames.map(_prefix + _)

      val stringIndexer = new StringIndexer()
        .setInputCols(inAttsNames)
        .setOutputCols(outAttsNames)
        .setHandleInvalid(handleInvalid)

      val features = outAttsNames ++ numericCols

        // au cas où pour retrouver le nom des features à partir des indices
      _featureIndices = features.zipWithIndex.map { case (f, i) => ("feature " + i, f) }


      //vectorAssembler
      val vectorAssembler = new VectorAssembler()
        .setInputCols(features)
        .setOutputCol(_featuresVec)
        .setHandleInvalid(handleInvalid)

      //VectorIndexer
      val vectorIndexer = new VectorIndexer()
        .setInputCol(_featuresVec)
        .setOutputCol(_featuresVecIndex)
        .setMaxCategories(maxCat)
        .setHandleInvalid(handleInvalid)

      val pipeline = new Pipeline()
        .setStages(Array(stringIndexer, vectorAssembler, vectorIndexer))

      pipeline
    }

    println("etape 6 declaration du pipeline")
    val maxCat = 270  //attention doit être >= aux nbre de valeurs distincts des attributs string (dt airport_id)
    val handleInvalid = "skip"

    val pipeline = AutoPipelineReg(StringCol, NumCol, maxCat, handleInvalid)

    //partitionnement du jeu de données en input du pipeline avec 2 fois plus de paritition que de plages horaires retenues
    println("Saisir nbre de partitions pour jointure:")
    val saisie_clavier4 = scala.io.StdIn.readLine()
    val numPartitions = saisie_clavier4.toInt
    print(s"nbre  text $numPartitions")

    raw_data.show()
    //pas possible de faire du rangepartitioner avec plus de 1 clé
    val dataWithPartitions = raw_data.repartition(numPartitions,$"ORIGIN_AIRPORT_ID",$"DEST_AIRPORT_ID")

    val data_enc = pipeline.fit(dataWithPartitions).transform(dataWithPartitions)
      .select(col(_featuresVecIndex), col("Target"))
      .withColumnRenamed("Target", _label)

    val nbre_lignes=data_enc.count()
    data_enc.show(false)
    println(s"etape 7 :nbre de lignes : $nbre_lignes")


    //Méthode 1  avec retrait du surplus de classe majoritaire

    // step 1 : count the occurrences of each label
    val labelCounts = data_enc.groupBy("label").count().collect()

    // Calculate the majority and minority class counts
    val maxCount = labelCounts.map(_.getLong(1)).max
    println(maxCount)
    val minCount = labelCounts.map(_.getLong(1)).min
    println(minCount)

    // Calculate the balancing ratio en double
    val balanceRatio = minCount.toDouble / maxCount.toDouble
    println(balanceRatio)

    // setp 3 : Create a sampling fraction map
    //val fractions = Map(true -> 1.0, false-> balanceRatio)  // qd la label est un boolean
    val fractions = Map(1 -> 1.0, 0 -> balanceRatio) // qd la label est un entier

    // Step 4 : Perform the resampling using the fractions
    val data_enc1 = data_enc.stat.sampleBy("label", fractions = fractions, seed = 42)
    println("etape 8    ")

    //contrôle val labelCounts = transformedDataset.groupBy("labelIndex").count().orderBy("labelIndex")
    val labelCounts1 = data_enc1.groupBy("label").count().collect()

    // Calculate the majority and minority class counts
    val maxCount1 = labelCounts1.map(_.getLong(1)).max
    println(s"nouvelle quantité max : $maxCount1")
    val minCount1 = labelCounts1.map(_.getLong(1)).min
    println(s"nouvelle quantité  min : $minCount1")

    //Last phase with the predictor

    //Step 1 : split data entre train et test
    val Array(trainingData_s0, testData_s0) = data_enc1.randomSplit(Array(0.7, 0.3))


    //Declaration of the classifier : here RandomForest
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxBins(maxCat)
      .setNumTrees(SaisieNumTree)
      .setMaxDepth(20)
      .setSubsamplingRate(0.5)
      .setMaxBins(270)
      .setMinInstancesPerNode(20)
      //.setFeatureSubsetStrategy("onethird")

    //println("valeur de maxCat :",maxCat)
    /*
    /////////////////////////////essai de split des randomforests
    // Split the data into subsets
    val division = 3
    val subsets_train = trainingData_s0.randomSplit(Array.fill(division)(1.0), seed = 42L)

    // Define an array to hold the sub-Random Forests
    val subForests_model = new Array[RandomForestClassificationModel](division)

    // Train sub-Random Forests
    for (i <- 0 until division) {
      val subset = subsets_train(i)
      subForests_model(i) = rf.fit(subset)
    }

    val subsets_test = testData_s0.randomSplit(Array.fill(division)(1.0), seed = 42L)
    val subForests_predictions = new Array[org.apache.spark.sql.DataFrame](division)

    // test sub-Random Forests
    for (i <- 0 until division) {
      val subset = subsets_test(i)
      subForests_predictions (i)= subForests_model(i).transform(subset)
    }
    val rf_predictions = subForests_predictions.reduce(_.union(_))
    ///////////////////////////
    */

    /*
    ////////////////
    // Declaration of the Gridsearch
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(60,70)) // Varying number of trees
      .addGrid(rf.maxDepth, Array(10,20)) // Varying maximum depth //max acceptable value <= 30
      .addGrid(rf.minInstancesPerNode, Array(20, 30)) //impact profondeur et complexité ..+ grd réduit le risque de surapprentissage
      //.addGrid(rf.minInfoGain, Array(0.05, 0.1)) //gain minimal d'info à chaque division  ..+ grd réduit le risque de surapprentissage
      //.addGrid(rf.maxBins, Array(270, 300)) //nbre de paquets pour découper intervals features continues attention doit être ai moins égal au max des catégo pour variables catégorielles ici 100
      //.addGrid(rf.featureSubsetStrategy, Array("auto", "sqrt", "log2"))   // Varying feature subset strategy
      //.addGrid(rf.subsamplingRate, Array(0.25, 0.5))
      .build()

    val cv = new CrossValidator()
      //.setEstimator(pipeline)
      .setEstimator(rf)
      .setEvaluator((new BinaryClassificationEvaluator).setMetricName("areaUnderROC")) //by default
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3) //   // Number of cross-validation folds
      .setParallelism(10) // Evaluate up to 2 parameter settings in parallel
    ////////////////////////////////
    */

    // Train model. This also runs the indexer.
    val rf_model = rf.fit(trainingData_s0)
    //val rf_model =cv.fit(trainingData_s0)

    val rf_predictions = rf_model.transform(testData_s0)

    // Select (prediction, true label) and compute test error.
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

    // Calculate AUC (Area Under the ROC Curve)
    //by default   .setMetricName("areaUnderPR")
    val auc = evaluator.evaluate(rf_predictions)



    // Calculate TP, FP, FN
    val tp = rf_predictions.filter($"prediction" === 1 && $"label" === 1).count().toDouble
    val fp = rf_predictions.filter($"prediction" === 1 && $"label" === 0).count().toDouble
    val fn = rf_predictions.filter($"prediction" === 0 && $"label" === 1).count().toDouble

    // Calculate precision and recall
    val precision = tp / (tp + fp)
    val recall = tp / (tp + fn)
    val f1score = 2*(precision*recall)/(precision +recall)

    println(s"AUC: $auc")
    println(s"Precision: $precision")
    println(s"Recall: $recall")
    println(s"F1score : $f1score")


    ////////////////////////////////////////////////
    // to extract the hyperparameters of the best model
    /*
    val bestParams = rf_model.getEstimatorParamMaps
      .zip(rf_model.avgMetrics)
      .maxBy(_._2)
      ._1

    val bestNumTrees = bestParams(rf.numTrees)
    val bestMaxDepth = bestParams(rf.maxDepth)
    val bestMinInstancesPerNode = bestParams(rf.minInstancesPerNode)
    //val bestMaxBins = bestParams(rf.maxBins)
    //val bestMSubSampling = bestParams(rf.subsamplingRate)


    println(s"Best numTrees: $bestNumTrees")
    println(s"Best maxDepth: $bestMaxDepth")
    println(s"Best minInstancesPerNode: $bestMinInstancesPerNode")
    //println(s"Best MaxBins: $bestMaxBins")
    //println(s"Best SubSampling: $bestMSubSampling")
    */
    /////////////////////////////////////////


      }
}
