package de.kp.works.quandl
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import de.kp.works.http.HttpConnect
import de.kp.works.spark.Session

class QuandlReader extends BaseReader with HttpConnect {
  
  private var apiKey:String = ""
  
  private var database:String = ""
  private var dataset:String = ""
  
  private var numSlices:Int = 1
  private val datatype:String = "json"
  
  private val session = Session.getSession
 
  def setApiKey(value:String):QuandlReader = {
    apiKey = value
    this
  }
  
  def setDatabase(value:String):QuandlReader = {
    database = value
    this
  }
  
  def setDataset(value:String):QuandlReader = {
    dataset = value
    this
  }

  def read:DataFrame = {

    try {
    
      /****************************************
       * 
       * Retrieve dataset from Quandl as JSON
       * 
       ***************************************/
  
      val endpoint = s"${base}/${database}/${dataset}.${datatype}?api_key=${apiKey}"
      val json = getJson(endpoint)
    
      /****************************************
       * 
       * Transform retrieved JSON into an Apache 
       * Spark DataFrame
       * 
       ***************************************/

      /* Infer (payload) fields */
      val fields = getFields(json)

      /* Build data rows */
      val rows = buildRows(json, fields)      

      /* Retrieve schema */
      val schema = StructType(Array(StructField("timestamp", LongType,   true)) 
          ++ fields)
        
      transform(rows, schema)
      
    } catch {
      case t:Throwable => {
        log.error(s"Extracting Quandl response failed with: " + t.getLocalizedMessage)
        session.emptyDataFrame
      }
    }
    
  }
  
  private def transform(rows:Seq[Row], schema:StructType):DataFrame = {
    
    val rdd = session.sparkContext.parallelize(rows, numSlices)
    val dataframe = session.createDataFrame(rdd, schema)
      .na.drop.sort(col("timestamp").asc)

    dataframe
    
  }
  
}