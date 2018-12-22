package com.self.dataAsService

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import com.google.gson.Gson
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.http.impl.client.HttpClientBuilder

object NifiRestCall {

def nifiCall( url: String ) {
  // create a StatusJSON object
  //val StatusJSON: JValue = ( "Status" -> "OK" )
  
  val StatusJSON: String = """{"status" : "true"}"""
  //val v: JValue = ( "Status" -> "OK" )
  // convert it to a JSON string
  //val finalJson = new Gson().toJson(StatusJSON)

  // create an HttpPost object
  //val post = new HttpPost("http://13.82.55.156:80/contentListener")
	val post = new HttpPost(url)
  // set the Content-type
  post.setHeader("Content-type", "application/json")

  // add the JSON as a StringEntity
  post.setEntity(new StringEntity(StatusJSON))
  //post.setEntity(StatusJSON)

  // send the post request
  val response = HttpClientBuilder.create().build().execute(post)

  // print the response headers
  println("--- HEADERS ---")
  response.getAllHeaders.foreach(arg => println(arg))
  
 }

}

class StatusJSON (var status: String, var value: String) {
  override def toString = status + ", " + value
}