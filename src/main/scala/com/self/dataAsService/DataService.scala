package com.self.dataAsService

import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc.UsernamePasswordToken
import org.apache.shiro.subject.Subject
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import com.self.dataAsService.QueryService._

import akka.actor.Actor
import akka.actor.Props
import org.apache.shiro.session.UnknownSessionException
import org.apache.shiro.authc.UnknownAccountException
import org.apache.shiro.authc.LockedAccountException
import org.apache.http.protocol.RequestConnControl
import spray.http._
import spray.http.MediaTypes._
import spray.http.StatusCodes._
import spray.routing._
import spray.routing.Directive
import org.apache.shiro.session.Session
import scala.concurrent.Await
import scala.concurrent.Future
import spray.client.pipelining.sendReceive
import spray.client.pipelining.sendReceive$default$3
import spray.client.pipelining.Post
import scala.concurrent.duration.DurationInt
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class DataServiceActor extends Actor with DataService {

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)

  RegisterReport.registerAllReports

}

// this trait defines our service behavior independently from the service actor
trait DataService extends HttpService {
  var sId: Option[Session] = None
  val eihurl = "eih.com"
  val timeout = 25.seconds
//  val system2 = ActorSystem("Rest-Client")
  val conf = ConfigFactory.load
  //implicit val formats = DefaultFormats
  //case class LoginJson(user_name: String, password: String)
  val myRoute = respondWithMediaType(MediaTypes.`application/json`) {

    path("api" / Segment) { (key) =>
      get { requestContext =>
        {
          if (key == "logout") {
            shutdown()
            complete("logged out")
          } else {
            val queryService = actorRefFactory.actorOf(Props(new QueryService(requestContext)))
            queryService ! QueryService.Process(key)
          }
        }
      }
    } ~ post {
      path("api" / Segment) { (key) =>
        {
          ctx =>
            {
              validate(ctx) match {

                case false => {
                  complete("INVALID")
                }
                case true => {
                  import QueryService._
                  println("$$$$$$$THE REQUEST CONTEXT: " + ctx)
                  if (key == "loginPage") {
                    sId = None
                    val logJson: LoginJson = loginJSON(ctx)

                    var user = logJson.user_name
                    println("This***********is ************the************user" + user)

                    var pass = logJson.password

                    val queryService = actorRefFactory.actorOf(Props(new QueryService(ctx)))
                    queryService ! QueryService.ProcessForPost(key)
                  } else {
                    println("in post else")
                    val queryService = actorRefFactory.actorOf(Props(new QueryService(ctx)))
                    queryService ! QueryService.ProcessForPost(key)
                  }
                } // end True
              } // end validate
            } //.apply(ctx) // end req
        }
      } // end path
    } // end post
  }

  def failureFunc: RequestContext => Unit = requestContext => {
    requestContext.complete(compact(render(jsonStat("failure", "null"))))
  }

  def ensureUserLoggedOut: Unit = {
    val curUser = SecurityUtils.getSubject
    if (curUser == null) {
      return
    }
    curUser.logout()
    val session = curUser.getSession(false)
    if (session == null) {
      return
    }
    session.stop()

  }

  def performUserValidation(successFunc: (RequestContext, String) => Unit, failureFunc: RequestContext => Unit) = {
    println("IN PERFORMUSERVALIDATION")
    optionalCookie("SESSION_ID") {
      case Some(sessionId) => {
        println("THE CURRENT SESSIONID IS: " + sessionId)
        successFunc(_, sessionId.value)
      } //.value.tokenizeSessionId)
      case None => failureFunc
    }
  }

  def validate(ctx: RequestContext): Boolean = {
    println("inside validate")
    //val cookiePresent: Boolean = ctx.request.cookies.find(_.name == ("eih_st_cookiename")) == Some
    val selectedCookieList: List[HttpCookie] = ctx.request.cookies.filter { cookie => (cookie.name == "eih_st_cookiename") }

    selectedCookieList.size match {
      case 0 => { println("Cookie Not present; Called from Standalone"); true }
      case _ => {
        val stCookieName: String = selectedCookieList(0).value.split("=")(1)
        println(s"stCookieName : $stCookieName")
        val serviceTicket: String = ctx.request.cookies.filter { cookie => (cookie.name == stCookieName) }(0).value.split("=")(1)
        println(s"serviceTicket : $serviceTicket")
        val casResponseService: CASResponse = validateCasServiceTicket("http://" + eihurl, serviceTicket)
        println(s"casResponseService : $casResponseService")
        casResponseService.errormessage match {
          case "" => { println("No Error message; Called from EIH with valid cookie: "); true }
          case _ => { println("Error message: Called from EIH with invalid cookie: "); false }
        }
      }
    }

  }

  def validateCasServiceTicket(serviceUrl: String, serviceTicket: String): CASResponse = {
    import org.json4s.JsonDSL._

    val casServiceUrl: String = conf.getString("eihServer.rest_url")
    println("\n\nCAS_URL: "+ casServiceUrl + "\n\n")
    val sys = Boot.dispatcherSource.get
    // ES REST service invocation
    import sys.dispatcher
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val jsonStr: String = s"""{"serviceURL":"$serviceUrl","serviceticket":"$serviceTicket"}"""
    println("json : " + jsonStr)
    val response: Future[HttpResponse] = pipeline(Post(casServiceUrl, jsonStr))
    val resultString = Await.result(response.map(_.entity.asString), timeout)
    println(s"Got the response: resultJSON")

    convertCASJson(resultString)
  }

  def convertCASJson(jsonStr: String): CASResponse = {
    import scala.util.parsing.json._
    val json = JSON.parseFull(jsonStr)
    val jsonMap = json.getOrElse("None").asInstanceOf[Map[String, Any]]

    val status: String = jsonMap.get("status").get.asInstanceOf[Double].toString()
    val message: String = jsonMap.get("message").get.asInstanceOf[String]
    val errormessage: Option[Any] = jsonMap.get("errormessage")
    errormessage match {
      case Some(errormesg) => CASResponse(status, message, errormessage.get.asInstanceOf[String])
      case None => CASResponse(status, message)
    }
    //CASResponse(status, message)
  }

  def shutdown() {
//    system2.shutdown()
  }
}
