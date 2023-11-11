package com.slim

//création des classes et encoders qui serviront dans la phase de map (en prévision de bascule de dataframe à dataset)

//Define class Join_Key
//case class Join_Key(ORIGIN_AIRPORT_ID: Int, Date: java.sql.Date)


//Define class Composite_Key
//case class Composite_Key(Join_key:Join_Key,Table_tag:String)
//case class Composite_Key(Join_key:Join_Key,Table_tag:String)

//Define class Flights
case class Flights(
                    ORIGIN_AIRPORT_ID: Int,
                    DEST_AIRPORT_ID: Int,
                    CRS_DEP_TIMESTAMP: java.sql.Timestamp,
                    //ACTUAL_DEPARTURE_TIMESTAMP: java.sql.Timestamp,
                    SCHEDULED_ARRIVAL_TIMESTAMP: java.sql.Timestamp,
                    //ACTUAL_ARRIVAL_TIMESTAMP: java.sql.Timestamp,
                    ARR_DELAY_NEW: Double)

//Define class Weather
case class Weather(
                    AIRPORT_ID: Int,
                    Weather_TIMESTAMP:java.sql.Timestamp,
                    DryBulbCelsius:Double,
                    SkyCOndition:String,
                    Visibility:Double,
                    //WindDirection:Int,
                    WindSpeed:Double,
                    WeatherType:String,
                    //StationPressure:Double)
                    HourlyPrecip:Double)