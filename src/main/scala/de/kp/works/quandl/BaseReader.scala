package de.kp.works.quandl
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.google.gson._
import scala.collection.mutable

trait BaseReader {

  /* "GET https://www.quandl.com/api/v3/datasets/{database_code}/{dataset_code}.{return_format} */
  protected val base = "https://www.quandl.com/api/v3/datasets"
    
  protected val day_formatter = new SimpleDateFormat("yyyy-MM-dd")

  protected def getFields(json:JsonElement):Array[StructField] = {

    val fields = mutable.ArrayBuffer.empty[StructField]
    val dataset = json.getAsJsonObject.get("dataset").getAsJsonObject

    val data = dataset.get("data").getAsJsonArray
    if (data.size == 0) return fields.toArray
    
    val columns = dataset.get("column_names").getAsJsonArray
    /*
     * In order to determine the data types, we must
     * evaluate field values that are different from
     * 'null'.
     * 
     * To avoid, that the first row already contains 'null'
     * values, we evaluate the first 10 rows
     */
    val (row, count) = findRow(data)
    if (row == null)
      throw new Exception(s"Unable to infer schema")

    /*
     * It is expected that the first element is `Date`.
     * This element is neglected as this method retrieves
     * the payload fields 
     */
    (1 until count).foreach(i => {

      val element = row.get(i)
      /*
       * Make sure that the element is a JSON primitive
       */
      if (element.isJsonPrimitive == false)
        throw new Exception(s"Non-basic data type detected.")
      
      /*
       * The current name is set to lower case and white spaces
       * are replaced by underscores; also extra characters must
       * be replaced
       */
      val column = columns.get(i).getAsString
        .toLowerCase
        .replace(" ", "_")
        .replace(":", "_")
        /* __MOD__ */
        .replace("(","")
        .replace(")","")

      /*
       * Evaluate the data type of the element
       */
      if (isDouble(element))
        fields += StructField(column, DoubleType, true)
      
      else if (isFloat(element))
        fields += StructField(column, FloatType, true)
      
      else if (isInteger(element))
        fields += StructField(column, IntegerType, true)

      else
        fields += StructField(column, StringType, true)
        
    })

    fields.toArray
    
  }
  
  protected def buildRows(json:JsonElement, fields:Array[StructField]):Seq[Row] = {

    val dataset = json.getAsJsonObject.get("dataset").getAsJsonObject
    val data = dataset.get("data").getAsJsonArray

    val rows = (0 until data.size).map(i => {

      val row = data.get(i).getAsJsonArray

      val datetime = row.get(0).getAsString      
      val timestamp = {
        val date = day_formatter.parse(datetime)
        date.getTime
      }
      
      val payload = (1 to fields.size).map(j => {
        
        val value = row.get(j)
        if (value.isJsonNull) null else {
          
          val datatype = fields(j-1).dataType
          datatype match {
            case DoubleType => 
              value.getAsDouble

            case FloatType => 
              value.getAsFloat

            case IntegerType => 
              value.getAsInt
              
            case StringType =>
              value.getAsString
            
            case _ => throw new Exception(s"Data type `${datatype.simpleString}` is not supported.")
          }
          
        }

       })
      
       val values = Seq(timestamp) ++ payload
       Row.fromSeq(values)
        
     })
    
     rows
    
  }
  
  private def findRow(data:JsonArray):(JsonArray, Int) = {
   
    var count = 0
    var row:JsonArray = null
    
    var size:Int = 0
    /*
     * Check the first 10 entries to determine either a truncated 
     * row (last item) or a row with no null values    
     */
    while (row == null && count < 10) {
      
      var isNull = false
      var isTruncated = false

      val sample = data.get(count).getAsJsonArray
      (0 until sample.size).foreach(i => {
        
        val e = sample.get(i)
        if (e.isJsonNull) {
          /*
           * This methods supported automated 
           * truncation of the provided rows
           */
          if (i != sample.size - 1) isNull = true
          else
            isTruncated = true
        }
      
      })
      
      if (isNull == false) {
        
        row = sample
        size = sample.size

        if (isTruncated == true)
          size = size - 1

      }
      else
        count += 1
    }
    
    (row, size)

  }
  
  private def isDouble(element:JsonElement):Boolean = {
    try {
      val value = element.getAsDouble
      true
    } catch {
      case t:Throwable => false
    }
  }
    
  private def isFloat(element:JsonElement):Boolean = {
    try {
      val value = element.getAsFloat
      true
    } catch {
      case t:Throwable => false
    }
  }
    
  private def isInteger(element:JsonElement):Boolean = {
    try {
      val value = element.getAsInt
      true
    } catch {
      case t:Throwable => false
    }
  }
  
}