package com.self.dataAsService

case class CASResponse(status: String, message: String, errormessage: String)

object CASResponse {
  def apply(status: String, message: String): CASResponse ={
    new CASResponse(status, message, "")
  }
}