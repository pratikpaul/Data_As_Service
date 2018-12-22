package com.self.dataAsService

import scala.collection.immutable.ListMap
import akka.actor._
import akka.actor.{ Actor, ActorRef }
//import akka.actor.Actor
import akka.event.Logging
import akka.io.IO

import spray.routing.RequestContext
import spray.httpx.SprayJsonSupport
import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc.UsernamePasswordToken
import scala.collection.JavaConverters._

import javax.servlet.http.Cookie
import java.text.SimpleDateFormat
import org.apache.shiro.subject.Subject
import spray.routing.Directive
import spray.http.HttpRequest
import spray.http.HttpCookie
import spray.http.HttpHeaders.RawHeader
import org.apache.shiro.session.UnknownSessionException
import org.apache.shiro.authc.UnknownAccountException
import org.apache.shiro.authc.LockedAccountException
import org.apache.http.protocol.RequestConnControl
import java.util.{ Date, Locale }
import scala.util.{ Success, Failure }

import scalikejdbc._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import spray.http.StatusCodes
import java.io.PrintWriter
import com.typesafe.config.ConfigFactory
import com.self.dataAsService.util._
import java.sql.SQLException
import com.self.eihDrive.EihController
import com.self.appData.AppController
import com.self.appData.dto.AccessInfo

object QueryService {
  case class Process(key: String)
  case class ProcessForPost(key: String)
  case class LoginJson(user_name: String, password: String)
  implicit val formats = DefaultFormats + new CustomNoneJNullSerializer()

  val conf = ConfigFactory.load

  def jsonStat(status: String, principal: String): JValue = {

    ("status" -> status) ~ ("message" -> principal)

  }

  def loginJSON(requestContext: RequestContext): LoginJson = {

    val entity_msg: String = requestContext.request.entity.asString

    val json = parse(entity_msg)

    val loginJson: LoginJson = json.extract[LoginJson]

    loginJson
  }
}

case class LoginInfo(user_id: Option[String], user_fullname: Option[String], user_name: Option[String],
  password: Option[String], mail_id: Option[String], contact_no: Option[String], is_admin: Option[String], is_subscriber: Option[String], is_publisher: Option[String])

case class DataServicesValues(user_id: Long, core_category_id: Long, core_subcategory_id: Long,
  metadata_core_id: Long, status: Option[Int] = None, service_type: String, start_date: String,
  row_delimiter: String, column_delimiter: String, is_encrypted: Option[Boolean] = None,
  is_header: Option[Boolean] = None, download_url: String, data_services_element: String,
  operation_flg: String, data_service_id: Int, data_service_name: String, share_type: String,
  is_ftp_share: Option[Boolean] = None, is_database_share: Option[Boolean] = None,
  ftp_share_path: Option[String] = None, ftp_username: Option[String] = None,
  ftp_password: Option[String] = None, database_type: Option[String] = None,
  database_server_ip: Option[String] = None, database_instance_name: Option[String] = None,
  database_username: Option[String] = None, database_password: Option[String] = None, filter: Option[String] = None)

case class Selected_columns(id: String, name: String, length: String, data_type: String)

case class DownloadDetails(categoryId: Int, subCategoryId: Int, userId: Int, downloadType: String, downloadStatus: String)
case class URLDownloadCols(subjectArea: String, datasetName: String, user: String)

case class UpdateAccessInfo(id: Int, role: String, access: String, user: String)

case class SendMessageToAll(fromUserId: String, message: String)

case class ReplyRequest(fromUserId: String, toUserId: String, refQueryId: String, message: String, status: String)

case class NewRequest(fromUserId: String, toUserId: String, message: String)

case class DeleteByQueryId(queryId: Int)

case class ServiceDetail(core_category_id: String, core_subcategory_id: String, metadata_core_id: String,
  status: Option[String], service_type: String, start_date: String, row_delimiter: String,
  column_delimiter: String, is_encrypted: Option[String], is_header: Option[String], data_service_id: Option[String] = None,
  data_service_name: String, share_type: String, is_ftp_share: Option[String], is_database_share: Option[String],
  ftp_share_path: Option[String], ftp_username: Option[String], ftp_password: Option[String],
  database_type: Option[String], database_server_ip: Option[String], database_instance_name: Option[String],
  database_username: Option[String], database_password: Option[String],
  selected_columns: List[Selected_columns], filter: Option[String])

case class ServiceDetailForCart(core_category_id: String, core_subcategory_id: String, metadata_core_id: String,
  status: Option[String], service_type: String, start_date: String, row_delimiter: String,
  column_delimiter: String, is_encrypted: Option[String], is_header: Option[String], data_service_id: Option[String] = None,
  data_service_name: String, share_type: String, is_ftp_share: Option[String], is_database_share: Option[String],
  ftp_share_path: Option[String], ftp_username: Option[String], ftp_password: Option[String],
  database_type: Option[String], database_server_ip: Option[String], database_instance_name: Option[String],
  database_username: Option[String], database_password: Option[String],
  selected_columns: List[Selected_columns], filter: Option[String], summary: String, description: String, importantColumns: String, unitPrice: Int, unitCurrency: String)

case class UserInfo(user_id: String, service_details: List[ServiceDetail])

case class UserInfoForCart(user_id: String, service_details: List[ServiceDetailForCart])

case class DataServiceUpdate(status: String, service_type: String, data_service_id: String)

case class UserEntry(first_name: String, middle_name: Option[String], last_name: String, user_id: String,
  password: String, mail_id: String, contact_no: String, is_admin: String,
  is_subscriber: String, is_publisher: String, is_searcher: String, is_ingester: String, sandboxlocation: String, roles: List[Int])

case class DownloadHistory(user_id: String, data_service_id: String,
  download_details: String, download_status: String)

case class TypeInfo(id: String, name: String, Type: String)
case class GroupInsert(groupName: String, parentName: String, createdBy: String)
case class InputIntAndString(id: Int, info: String, readFlag: Int)
case class GroupUserDelete(groupId: Int, user: String)
case class GroupUserMapTable(id: String, userId: String, groupId: String, markDelete: String, createdBy: String,
  creationDate: String, modifiedBy: String, modificationDate: String)
case class GroupUserSubjectAreaTable(id: String, subCategoryId: String, groupId: String, markDelete: String, createdBy: String,
  creationDate: String, modifiedBy: String, modificationDate: String)

case class UidIsAdmin(userId: Int, isAdmin: Boolean)
case class UidIsAdminSearch(userId: Int, isAdmin: Boolean, searchTerm: String, showUnmapped: Boolean)
case class MapIds(id: Int)
case class EihInputHibernate(uid: Int, roleId: Int, schema: String)
case class AppDataInputHibernate(accessType: String, user: String, schema: String)
case class MapGroupToSubjectArea(groupId: Int, createdBy: String, subCategoryIds: List[MapIds])
case class MapSubjectAreaToGroup(subCategoryId: Int, createdBy: String, groupIds: List[MapIds])
case class MapSubjectAreaListToGroup(subCategoryIds: List[Int], createdBy: String, groupIds: List[MapIds])
case class MapGroupToUser(userId: Int, createdBy: String, groupIds: List[MapIds])
case class ApproveOrReject(subscribeCartId: Int, approveOrRejectStatus: Int, userId: Int, userName: String, data_service_name: String, ftp_username: String, ftp_password: String, filterCond: String, comment: String)
case class SubscribeCart(id: Int, user_id: Int, core_category_id: Int, category_name: String, core_subcategory_id: Int,
  datasetName: String, shareType: String, serviceType: String, column_delimeter: String, row_delimeter: String, filter: String,
  selected_columns: String, ftp_share_path: String, MarkDelete: Int, CreatedBy: String, CreationDate: Date,
  SubscribeOn: Date, IsSubscribe: Int, ApproveStatus: Int, ApprovedOrRejectBy: String, ApproveOrRejectDate: Date,
  ApproverOrRejectorID: Int, is_encrypted: Boolean, is_header: Boolean, metadata_core_id: Int)

case class SubscribeCartAllString(id: Int, user_id: String, core_category_id: String, category_name: String, core_subcategory_id: String, datasetName: String, shareType: String, serviceType: String, column_delimeter: String, row_delimeter: String, filter: String, selected_columns: String, ftp_share_path: String, MarkDelete: String, CreatedBy: String, CreationDate: String, SubscribeOn: String, IsSubscribe: String, ApproveStatus: String, ApprovedOrRejectBy: String, ApproveOrRejectDate: String, ApproverOrRejectorID: String, is_encrypted: String, is_header: String, metadata_core_id: String)

case class SchemaInfo(hive_database_name: String, hbase_table: String)
case class QuickView(schema_table_info: List[SchemaInfo])
case class QuickViewVar(database_name: String, table_name: String, condition: String)

case class DataCatalogInfos(categoryId: Int, categoryName: String, mappedDatasetCount: Int, unMappedDatasetCount: Int, publisherCount: Int, subscriberCount: Int, DownloadCount: Int)
case class DataCatalogDetailsByName(categoryId: Int, categoryName: String, coreCategoryId: Int, coreCategoryName: String)
case class CatIdAndCount(catName: String, catId: Int, count: Int)
case class CatIdAndCountByName(catName: String, catId: Int, subCatId: Int, subCatName: String, count: Int)
case class CustomerDetails(checked: Boolean, name: String, id: Int, Type: String, Subject_area: String, createdBy: Option[String])
case class SubjectAreaDetails(columnName: List[CustomerDetails])
case class PublishSubjectArea(colDetails: SubjectAreaDetails*)
case class PublishDatasets(coreSubCategoryId: Int, createdBy: String, userId: Int, groupIds: List[MapIds], summary: String, description: String, importantColumns: String, unitPrice: Int, unitCurrency: String)
case class CoreSubCatIdAndUserInfo(coreSubCategoryId: Int, createdBy: Option[String], userId: Int)

case class FilterCond(filter: String, selectedColumn: String, condition: String, parentheses_1: String, parentheses_2: String, value: String, relationaloperator: String)
case class TestRule(subCategoryId: String, filterCondition: List[FilterCond])

case class FtpInfo(ftpHost: String, ftpUser: String, ftpPassword: String, localFilePath: String, remotePath: String, subjectArea: String, datasetName: String, user: String)

case class FTPDetails(id: Int, ftp_name: String, ftp_server: String, ftp_user_name: String, ftp_password: String, ftp_port: String, ftp_default_location: String)

case class SandBox(user: String, fileName: String, delimiter: String, subjectArea: String, datasetName: String, sandBoxID: String)

case class MappedDatasetSpec(categoryId: Int, userId: Int)

trait Update
case class UpdateUser(id: String, first_name: String, middle_name: Option[String],
  last_name: String, user_id: String, password: String, mail_id: String, contact_no: String,
  created_date: String, is_admin: String, is_subscriber: String, is_publisher: String, is_searcher: String, is_ingester: String, sandboxlocation: String, roles: List[Int]) extends Update

case class UpdateReadFlag(queryId: String) extends Update

case class DownloadUrlPath(urlPath: String) extends Update

case class InsertScore(user_id: String, rating: String, subcategory_id: String, commen_header: String, comments: String)

case class CoreSubCatJoinGroupSubjAreaMap(groupId: Int, groupName: String)
case class CoreSubCatJoinPublishedSubj(published_subjects_id: Int, status: String, visibility: Boolean, summary: String, description: String, important_columns: String, unit_price: Double, unit_currency: String, createdBy: String, creationDate: String)
case class CoreCatJoinCoreSub(core_subcategory_id: Int, core_subcategory_name: String, rating: Int, groupSubjectAreaMap_info: List[CoreSubCatJoinGroupSubjAreaMap], published_subjects_info: List[CoreSubCatJoinPublishedSubj], publisherCount: Int, subscriberCount: Int, downloadCount: Int, gdprCount: Int)
case class CoreCatSubCatGroupSubjPubSubj(core_category_id: Int, core_category_name: String, core_subcategory_info: List[CoreCatJoinCoreSub])

case class EihAccessInfoDetails(uid: Int, roleId: List[Int])
case class AppAccessInfoDetails(appId: Int, role: String, access: String, user: String)

case class GDPRRuleArray(columnID: Int, columnName: String, isVisible: Boolean, GDPRRuleID: Int, GDPRRuleName: String)
case class GDPRColumnMap(subjectareaID: Int, subjectareaName: String, datasetID: Int, datasetName: String, createdBy: String, gdprRuleArray: List[GDPRRuleArray])

class QueryService(requestContext: RequestContext) extends Actor with DataService {

  import QueryService._
  import DatabaseConn._
  def actorRefFactory = context

  implicit val system = context.system
  import system.dispatcher
  implicit val formats = DefaultFormats
  val log = Logging(system, getClass)
  val queryParams: Map[String, String] = ListMap(requestContext.request.message.uri.query.toMap.toSeq.sortBy(_._1): _*)
  var s: String = requestContext.request.message.uri.toString

  val eihController = EihController.getInstance(dataManipulationJdbcUrl, eihDataSchema)

  val appController = AppController.getInstance(dataManipulationJdbcUrl, appDataSchema)

  println("REQUEST_CONTEXT----->" + requestContext.request.message.uri.query.toMap.toSeq.sortBy(_._1))
  println("======>" + queryParams)
  println("this is the uri ------++++++++>>>>>>" + s.substring(s.lastIndexOf("/") + 1))

  println("List======>" + queryParams.values.toList)

  def receive = {
    case Process(key) => process(key)
    case ProcessForPost(key) =>
      processForPost(key)
      context.stop(self)
  }

  def processForPost(key: String) = {

    import spray.http.StatusCodes._

    var fetchUri: String = requestContext.request.message.uri.toString

    val entity_msg: String = requestContext.request.entity.asString

    val json = parse(entity_msg)

    key match {

      case "saveGDPRColumn" => {
        val extractor = json.extract[GDPRColumnMap]
        try {
          if (Drive.isGDPRPresent(extractor.subjectareaID, extractor.datasetID)) {
            val id = Drive.getIdfromSubAreaAndDataSet(extractor.subjectareaID, extractor.datasetID)
            Drive.updateGDPRColumnMap(id, extractor.createdBy)
          }

          Drive.saveGDPRColumnMap(extractor)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "updateAccessInfo" => {
        val extractor = json.extract[UpdateAccessInfo]
        val id = extractor.id
        val role = extractor.role
        val access = extractor.access
        val user = extractor.user
        try {
          appController.updateAccessInfo(id, role, access, user)

          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "deleteAccessInfo" => {
        val extractor = json.extract[MapIds]
        val id = extractor.id
        val roleId = Drive.getRoleIdFromAccessInfoById(id)
        println("roleId retreived: " + roleId)
        try {
          appController.logicalDeleteAccessInfoById(id)
          println("---step2----")
          eihController.inactivateEihAccessInfoByRoleId(roleId)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "insertIntoAppRolesAndAccessInfo" => {
        val extractor = json.extract[AppAccessInfoDetails]
        val appId = extractor.appId
        val role = extractor.role
        val access = extractor.access
        val user = extractor.user
        var roleId = 0
        try {
          eihController.insertIntoEihAppRoles(appId)
          roleId = eihController.getNewlyCreatedRoleIdFromEihAppRoles(appId)

          appController.insertIntoAccessInfo(roleId, role, access, user)

          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getRolesFromAccessInfoByUid" => {
        val extractor = json.extract[MapIds]
        val uid = extractor.id
        var listOfRoleIds: List[Int] = Nil
        try {
          val eihAccessInfos = eihController.getAllEihAccessInfosByUID(uid)
          for (i <- 0 until eihAccessInfos.size()) {
            listOfRoleIds = listOfRoleIds :+ eihAccessInfos.get(i).getRole_id
          }
          val roles = listOfRoleIds.foldLeft("")((c, r) => { c + appController.getAccessInfoByRoleId(r).getRole + "," }).dropRight(1)
          val result: JValue = ("roles" -> roles)
          requestContext.complete(compact(render(result)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getRoleIfInAccessInfo" => {
        val extractor = json.extract[Process]
        val access = extractor.key
        try {
          val role = appController.getAccessInfoRole(access)
          val result: JValue = ("role" -> role)
          requestContext.complete(compact(render(result)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "insertIntoEihAccessInfo" => {
        val extractor = json.extract[EihAccessInfoDetails]
        val uid = extractor.uid
        val roleId = extractor.roleId
        try {
          Drive.insertInfoEihAccessInfoByUid(uid, roleId, eihController)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "insertIntoAppRolesAndGetRoleId" => {
        val extractor = json.extract[MapIds]
        val appId = extractor.id

        try {
          eihController.insertIntoEihAppRoles(appId)
          val roleId = eihController.getNewlyCreatedRoleIdFromEihAppRoles(appId)
          val result: JValue = ("roleId" -> roleId)
          requestContext.complete(compact(render(result)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getAccessInfoByAccessType" => {
        val extractor = json.extract[Process]
        val accessType = extractor.key
        try {
          val accessInfo = appController.getAccessInfoByAccessType(accessType)
          val result = appController.getJsonFromObject(accessInfo)
          requestContext.complete(result)
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getEIHAppIdByAppName" => {
        val extractor = json.extract[Process]
        val appName = extractor.key
        try {

          val eihApps = eihController.getFromEihAppsByAppName(appName)
          val result: JValue = ("app_id" -> eihApps.getApp_id())
          requestContext.complete(compact(render(result)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }

      }

      case "searchByCatNameOrSubCatName" => {
        val uidIsAdminSearch = json.extract[UidIsAdminSearch]
        try {
          val results = Drive.postSearchByCatAndSubCatName(uidIsAdminSearch)
          val resultsJvalue = Extraction.decompose(results)
          requestContext.complete(compact(render(resultsJvalue)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getPublisherDatasetSpec" => {
        val mappedDatasetExtract = json.extract[MappedDatasetSpec]
        val catId = mappedDatasetExtract.categoryId
        val userId = mappedDatasetExtract.userId
        val report = Drive.getReport("getMappedSubjectAreaForPublisher")
        try {

          var inQuery = report._1 + s""" WHERE C.user_id=${userId} AND C.Status='published'  AND b.id = ${catId}
                                  | ) C  WHERE C.Subject_area_id IN (SELECT SubCategoryId FROM groupSubjectAreaMap WHERE GroupId IN (
                                  | SELECT GroupId FROM groupUserMap WHERE UserId = ${userId} AND MarkDelete=0
                                  | ) AND SubCategoryId IN (SELECT core_subcategory_id FROM published_subjects WHERE MarkDelete=0 AND LOWER(Status) = 'published' AND user_id = ${userId}) 
                                  | AND MarkDelete=0)""".stripMargin

          var notInQuery = report._1 + s""" WHERE b.id = ${catId}) C  WHERE C.Subject_area_id IN (SELECT SubCategoryId FROM groupSubjectAreaMap WHERE GroupId IN (
                                  | SELECT GroupId FROM groupUserMap WHERE UserId = ${userId} AND MarkDelete=0
                                  | ) AND SubCategoryId NOT IN (SELECT core_subcategory_id FROM published_subjects WHERE MarkDelete=0 AND LOWER(Status) = 'published' AND user_id = ${userId})
                                  | AND MarkDelete=0)""".stripMargin

          val finalquery = inQuery + " UNION " + notInQuery

          val result = Drive.runGeneralQuery(finalquery, report._2)

          println(result)
          requestContext.complete(compact(render(result)))

        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getAdminMappedDatasetSpec" => {
        val mappedDatasetExtract = json.extract[MapIds]
        try {
          val coreCatJoinCoreSub = Drive.postAdminDatasetDetails(mappedDatasetExtract)
          requestContext.complete(compact(render(Extraction.decompose(coreCatJoinCoreSub))))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getUnmappedDataset" => {
        val report = Drive.getReport(key)
        try {
          val result = Drive.runGeneralQuery(report._1, report._2)
          requestContext.complete(compact(render(result)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "compactSubjectAreInfo" => {
        val uidIsAdmin = json.extract[UidIsAdmin]
        try {
          val results = Drive.postCompactSubjectAreaInfo(uidIsAdmin)
          val resultsJvalue = Extraction.decompose(results)
          requestContext.complete(compact(render(resultsJvalue)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "updateSubscribeCartMarkRead" => {
        val subscribeCartInfo = json.extract[MapIds]
        try {
          Drive.postUpdateSubscribeCartMarkRead(subscribeCartInfo)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "testEihHibernate" => {
        val inputIds = json.extract[EihInputHibernate]
        try {
          val eihController = EihController.getInstance(DatabaseConn.dataManipulationJdbcUrl, DatabaseConn.eihDataSchema)
          eihController.insertIntoEihAccessInfo(inputIds.uid, inputIds.roleId)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "testApplicationDataHibernate" => {
        val input = json.extract[AppDataInputHibernate]
        try {
          val appController = AppController.getInstance(DatabaseConn.dataManipulationJdbcUrl, DatabaseConn.appDataSchema)
          appController.insertIntoAccessPrivileges(input.accessType, input.user)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "deleteFtpDetailById" => {
        val deleteId = json.extract[MapIds]
        try {
          Drive.postDeleteFtpDetails(deleteId)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "editFtpDetails" => {
        val ftpDetails = json.extract[FTPDetails]
        try {
          Drive.postEditFtpDetails(ftpDetails)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "saveFtpDetails" => {
        val ftpDetails = json.extract[FTPDetails]
        try {
          Drive.postSaveFtpDetails(ftpDetails)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getAppConfigData" => {
        val appConfigInput = json.extract[Process]
        val report = Drive.getReport(key)
        val appKey = appConfigInput.key
        val query = report._1 + s""" AND AppKey = '${appKey}'"""

        val result = Drive.runGeneralQuery(query, report._2)
        requestContext.complete(compact(render(result)))
      }

      case "getCartDataByStatus" => {
        val statusInfo = json.extract[InputIntAndString]
        val report = Drive.getReport("getCartData")
        val id = statusInfo.id
        val readFlag = statusInfo.readFlag
        val status = statusInfo.info.toInt
        val allStatusQuery = s" AND user_id = ${id} AND (ApproveStatus = 0 OR ApproveStatus = 1 OR ApproveStatus = 2)"
        val allStatusQueryWithMarkRead = s" AND user_id = ${id} AND Mark_Read =${readFlag} AND (ApproveStatus = 0 OR ApproveStatus = 1 OR ApproveStatus = 2)"
        val selectiveStatusQuery = s" AND user_id = ${id} AND ApproveStatus = ${status}"
        val selectiveStatusQueryWithMarkRead = s" AND user_id = ${id} AND Mark_Read =${readFlag} AND ApproveStatus = ${status}"
        var query = report._1
        //if readFlag is -1 then retrieve all else retrieve based on Mark_Read column
        readFlag match {
          case -1 => {
            status match {
              case -1 => { query += allStatusQuery }
              case _ => { query += selectiveStatusQuery }
            }
          }
          case _ => {
            status match {
              case -1 => { query += allStatusQueryWithMarkRead }
              case _ => { query += selectiveStatusQueryWithMarkRead }
            }
          }
        }
        /*status match {
          case -1 => { query += s" AND user_id = ${id} AND Mark_Read =  AND (ApproveStatus = 0 OR ApproveStatus = 1 OR ApproveStatus = 2)" }
          case _ => { query += s" AND user_id = ${id} AND ApproveStatus = ${status}" }
        }*/

        val result = Drive.runGeneralQuery(query, report._2)
        requestContext.complete(compact(render(result)))
      }

      case "approveOrReject" => {
        val extractedJson = json.extract[List[ApproveOrReject]]
        try {
          val result = Drive.approveOrReject(extractedJson)
          val successInfo: JValue = ("success" -> "OK")

          requestContext.complete(compact(render(successInfo)))
          //				NifiRestCall.nifiCall("http://13.82.55.156:80/contentListener")
          MailService.notifyAdmin()
        } catch {
          case sqlEx: SQLException => {
            val msg = jsonStat("Failed", sqlEx.getMessage)
            requestContext.complete(compact(render(msg)))
          }
          case ex: Exception => {
            val msg: JValue = jsonStat("Failed", ex.getMessage)
            requestContext.complete(compact(render(msg)))
          }
        }
      }

      case "getAllUnApprovedCartData" => {
        val extractedJson = json.extract[MapIds]
        val userId = extractedJson.id
        val report = Drive.getReport("getCartData")
        val query = report._1 + s""" AND A.MarkDelete=0 AND A.IsSubscribe=0 AND A.ApproveStatus =0 AND A.user_id IN(SELECT DISTINCT userId 
                                | FROM groupUserMap AS C WHERE C.GroupId IN (SELECT GroupId 
                                | FROM groupUserMap AS D WHERE D.MarkDelete=0 AND D.UserId = ${userId}) AND C.UserId IN (SELECT user_id 
                                | FROM subscribeCart AS E WHERE E.MarkDelete=0 AND E.IsSubscribe=0 AND E.ApproveStatus =0))""".stripMargin

        val result = Drive.runGeneralQuery(query, report._2)
        requestContext.complete(compact(render(result)))
      }

      case "applyForSubscription" => {
        val extractedJson = json.extract[List[MapIds]]

        try {
          Drive.applyForSubscription(extractedJson)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => {
            val msg = jsonStat("Failed", sqlEx.getMessage)
            requestContext.complete(compact(render(msg)))
          }
          case ex: Exception => {
            val msg: JValue = jsonStat("Failed", ex.getMessage)
            requestContext.complete(compact(render(msg)))
          }
        }
      }

      case "deleteFromCart" => {
        val extractedJson = json.extract[MapIds]

        try {
          Drive.postDeleteFromCart(extractedJson)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getFromCartByIdForPublisher" => {
        val cartJson = json.extract[MapIds]
        val userId = cartJson.id
        val report = Drive.getReport("getFromCartByIdForSubscriber")

        var query = report._1 + s""" AND MarkDelete = 0 AND IsSubscribe = 0 AND ApproveStatus = 0 AND user_id IN(SELECT DISTINCT UserId 
                                | FROM groupUserMap WHERE groupId IN (SELECT groupId FROM groupUserMap WHERE UserId = ${userId}))""".stripMargin

        try {
          val result = Drive.runGeneralQuery(query, report._2)

          requestContext.complete(compact(render(result)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getFromCartByIdForSubscriber" => {
        val cartJson = json.extract[MapIds]
        val userId = cartJson.id
        val report = Drive.getReport(key)

        var query = report._1 + s" AND MarkDelete = 0 AND IsSubscribe = 0 AND ApproveStatus = -1 AND user_id = ${userId}"

        try {
          val result = Drive.runGeneralQuery(query, report._2)

          requestContext.complete(compact(render(result)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }

      }

      case "addToCart" => {
        val addToCart = json.extract[UserInfoForCart]
        val selectedColumnsJValue = (json \ "service_details" \ "selected_columns")
        val selectedColumns = compact(selectedColumnsJValue)
        try {
          Drive.postAddToCart(addToCart, selectedColumns)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case ex: SQLException => { requestContext.complete(ex.getMessage) }
          case e: Exception => { requestContext.complete(e.getMessage) }
        }

      }

      case "postAllSubjectAreas" => {
        val postAllSubjectAreas = json.extract[MapIds]
        val userId = postAllSubjectAreas.id
        val report = Drive.getReport("getAllSubjectArea")

        val query = report._1 + s""" WHERE C.Subject_area_id IN (SELECT DISTINCT core_subcategory_id FROM published_subjects A,publishedDatasetGroupMapping B WHERE 
                                | A.id = B.publishID AND B.Status = 'published' AND B.MarkDelete=0 AND B.groupID AND A.MarkDelete=0 AND LOWER(A.Status)='published' AND
                                | B.GroupID IN (SELECT GroupId FROM `groupUserMap` WHERE UserId = ${userId} AND MarkDelete = 0))""".stripMargin

        val result = Drive.runGeneralQuery(query, report._2)

        val finalJson = CommonProcesses.getFinalJsonWithSchemaAndColumnName(result)

        requestContext.complete(finalJson)
      }

      case "changeVisibility" => {
        val visibilityChange = json.extract[PublishDatasets]

        Drive.postChangeVisibility(visibilityChange)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "getPublishedDatasetGroupMapping" => {
        val publishedDatasetGroupMapping = json.extract[CoreSubCatIdAndUserInfo]
        val coreSubCategoryId = publishedDatasetGroupMapping.coreSubCategoryId
        val userId = publishedDatasetGroupMapping.userId
        val report = Drive.getReport(key)
        val publishId = (DB readOnly { implicit session =>
          sql"""SELECT id FROM published_subjects WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".map(rs => rs.int("id")).single.apply()
        }).get

        val query = report._1 + s" AND publishID = $publishId AND MarkDelete = 0 AND Status = 'published'"

        val v = Drive.runGeneralQuery(query, report._2)

        requestContext.complete(compact(render(v)))

      }

      case "unpublishDatasets" => {
        val unpublishDatasets = json.extract[CoreSubCatIdAndUserInfo]

        Drive.postUnpublishDataset(unpublishDatasets)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "publishDatasets" => {
        val publishDatasets = json.extract[PublishDatasets]

        Drive.postPublishDatasets(publishDatasets)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "getMappedSubjectAreaForPublisher" => {
        val userId = json.extract[MapIds].id
        val report = Drive.getReport(key)

        var inQuery = report._1 + s""" WHERE C.user_id=$userId AND C.Status='published'
                                  | ) C  WHERE C.Subject_area_id IN (SELECT SubCategoryId FROM groupSubjectAreaMap WHERE GroupId IN (
                                  | SELECT GroupId FROM groupUserMap WHERE UserId = ${userId} AND MarkDelete=0
                                  | ) AND SubCategoryId IN (SELECT core_subcategory_id FROM published_subjects WHERE MarkDelete=0 AND LOWER(Status) = 'published' AND user_id = ${userId}) 
                                  | AND MarkDelete=0)""".stripMargin
        val jsonForPublished = Drive.runGeneralQuery(inQuery, report._2)

        var notInQuery = report._1 + s""") C  WHERE C.Subject_area_id IN (SELECT SubCategoryId FROM groupSubjectAreaMap WHERE GroupId IN (
                                  | SELECT GroupId FROM groupUserMap WHERE UserId = ${userId} AND MarkDelete=0
                                  | ) AND SubCategoryId NOT IN (SELECT core_subcategory_id FROM published_subjects WHERE MarkDelete=0 AND LOWER(Status) = 'published' AND user_id = ${userId})
                                  | AND MarkDelete=0)""".stripMargin

        val jsonForYetToBePublished = Drive.runGeneralQuery(notInQuery, report._2)

        val finalJSONStringForPublished = CommonProcesses.getFinalJsonWithSchemaAndColumnName(jsonForPublished)
        val finalJSONStringForYetToBePublished = CommonProcesses.getFinalJsonWithSchemaAndColumnName(jsonForYetToBePublished)

        val finalJson = "{\"AlreadyPublished\" : " + finalJSONStringForPublished + ", \"YetToBePublished\" : " + finalJSONStringForYetToBePublished + "}"
        println(finalJson)
        requestContext.complete(finalJson)
      }

      case "getUserGroupList" => {
        try {
          val userGroupExtract = json.extract[MapIds]
          val report = Drive.getReport(key)
          val userId = userGroupExtract.id
          var query = report._1

          userId match {
            case x if x > -1 => { query += s" AND c.id = ${userId}" }
            case _ => {}
          }

          val v = Drive.runGeneralQuery(query, report._2)

          requestContext.complete(compact(render(v)))
        } catch {
          case e: Exception => {
            e.printStackTrace()
            requestContext.complete(e.getMessage)
          }
        }

      }

      case "getSubCategoryGroupListBySubCatId" => {
        try {
          val subCategoryGroupExtract = json.extract[MapIds]
          val report = Drive.getReport("getSubCategoryGroupList")
          val subCatId = subCategoryGroupExtract.id
          var query = report._1

          subCatId match {
            case x if x > -1 => { query += s" AND c.id = ${subCatId}" }
            case _ => {}
          }

          val v = Drive.runGeneralQuery(query, report._2)

          requestContext.complete(compact(render(v)))
        } catch {
          case e: Exception => {
            e.printStackTrace()
            requestContext.complete(e.getMessage)
          }
        }

      }

      case "getSubCategoryGroupList" => {
        try {
          val subCategoryGroupExtract = json.extract[MapIds]
          val report = Drive.getReport(key)
          val groupId = subCategoryGroupExtract.id
          var query = report._1

          groupId match {
            case x if x > -1 => { query += s" AND b.id = ${groupId}" }
            case _ => {}
          }

          val v = Drive.runGeneralQuery(query, report._2)

          requestContext.complete(compact(render(v)))
        } catch {
          case e: Exception => {
            e.printStackTrace()
            requestContext.complete(e.getMessage)
          }
        }

      }

      case "mapSubjectAreaToGroup" => {
        val subjectAreaToGroupMapper = json.extract[MapSubjectAreaToGroup]
        try {
          Drive.postMapSubjectAreaToGroup(subjectAreaToGroupMapper)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }
      
      case "mapSubjectAreaListToGroup" => {
        val extractor = json.extract[MapSubjectAreaListToGroup]
        try{
          Drive.postMapSubjectAreaListToGroup(extractor)
          
          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        }catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "mapGroupToSubjectArea" => {
        val groupToSubjectAreaMapper = json.extract[MapGroupToSubjectArea]

        try {
          Drive.postMapGroupToSubjectArea(groupToSubjectAreaMapper)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "mapGroupToUser" => {
        val groupToUserMapper = json.extract[MapGroupToUser]

        Drive.postMapGroupToUser(groupToUserMapper)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "userGroupDelete" => {

        val groupUserDelete = json.extract[GroupUserDelete]

        Drive.postUserGroupDelete(groupUserDelete)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "userGroupRename" => {

        val groupRename = json.extract[InputIntAndString]

        Drive.postRenameGroupName(groupRename)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "userGroupInsert" => {

        val groupInsert: GroupInsert = json.extract[GroupInsert]

        Drive.postInsertIntoGroup(groupInsert)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "sandBoxServe" => {
        var sandBoxInfo: SandBox = json.extract[SandBox]
        var filePath = ""
        val coreCategoryId = Drive.getCoreCategoryIdByName(sandBoxInfo.subjectArea)
        val coreSubCategoryId = Drive.getCoreSubCategoryIdByName(sandBoxInfo.datasetName)
        val userId = Drive.getUserIdByUser(sandBoxInfo.user)
        try {
          filePath = Drive.sandboxService(sandBoxInfo.user, sandBoxInfo.fileName, sandBoxInfo.delimiter, sandBoxInfo.subjectArea, sandBoxInfo.datasetName, sandBoxInfo.sandBoxID)
          Drive.insertIntoDownloadDetails(DownloadDetails(coreCategoryId, coreSubCategoryId, userId, "sandbox", "Success"))
          requestContext.complete(s"""{"hadoopFilePath" : "${filePath}"}""")
        } catch {
          case ex: Exception =>
            { ex.printStackTrace }
            requestContext.complete("""{"Status": "Failed..Please check log for error details"}""")
        }

      }

      case "URLDownload" => {
        val urlDownloadInfo: URLDownloadCols = json.extract[URLDownloadCols]

        val coreCategoryId = Drive.getCoreCategoryIdByName(urlDownloadInfo.subjectArea)
        val coreSubCategoryId = Drive.getCoreSubCategoryIdByName(urlDownloadInfo.datasetName)
        val userId = Drive.getUserIdByUser(urlDownloadInfo.user)
        try {
          Drive.insertIntoDownloadDetails(DownloadDetails(coreCategoryId, coreSubCategoryId, userId, "url", "Success"))
          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case ex: Exception =>
            { ex.printStackTrace }
            requestContext.complete("""{"Status": "Failed..Please check log for error details"}""")
        }
      }

      case "ftpUpload" => {
        var ftpInfo: FtpInfo = json.extract[FtpInfo]

        val coreCategoryId = Drive.getCoreCategoryIdByName(ftpInfo.subjectArea)
        val coreSubCategoryId = Drive.getCoreSubCategoryIdByName(ftpInfo.datasetName)
        val userId = Drive.getUserIdByUser(ftpInfo.user)

        val response = FTPProcessor.ftpUpload(ftpInfo.ftpHost, ftpInfo.ftpUser, ftpInfo.ftpPassword, ftpInfo.localFilePath, ftpInfo.remotePath)
        Drive.insertIntoDownloadDetails(DownloadDetails(coreCategoryId, coreSubCategoryId, userId, "ftp", "Success"))

        requestContext.complete(StatusCodes.OK)
      }

      case "deleteByQueryId" => {
        var deleteByQueryId: DeleteByQueryId = json.extract[DeleteByQueryId]

        Drive.deleteByQueryId(deleteByQueryId)

        deleteByQueryId = null

        requestContext.complete("""{"Status": "Ok"}""")
      }
      case "testRule" => {

        val testRule: TestRule = json.extract[TestRule]

        val response = Drive.postTestRule(testRule)

        requestContext.complete(response)
      }

      case "publishSubjectArea" => {
        val publishSubjectArea: PublishSubjectArea = json.extract[PublishSubjectArea]

        Drive.postSubjectArea(publishSubjectArea)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "newRequest" => {
        val newRequest: NewRequest = json.extract[NewRequest]

        Drive.postNewRequest(newRequest)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))

      }
      case "replyRequest" => {
        val replyRequest: ReplyRequest = json.extract[ReplyRequest]

        Drive.postReplyRequest(replyRequest)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "sendMessageToAll" => {
        val sendMsg: SendMessageToAll = json.extract[SendMessageToAll]

        Drive.postSendToAll(sendMsg)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }
      case "insertScore" => {
        val insertScore: InsertScore = json.extract[InsertScore]

        Drive.postInsertScore(insertScore)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "updateUser" => {
        val updateUser: UpdateUser = json.extract[UpdateUser]
        val uid = updateUser.id.toInt
        val roles = updateUser.roles
        try {
          Drive.postUpdate(updateUser)
          Drive.insertInfoEihAccessInfoByUid(uid, roles, eihController)

          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }

      }

      case "updateReadFlag" => {
        val updateReadFlag: UpdateReadFlag = json.extract[UpdateReadFlag]

        Drive.postUpdate(updateReadFlag)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }

      case "insertData" => {

        println("THE POST REQUESTCONTEXT TEST---------------<><><><>" + requestContext.request.entity.asString)

        val user: UserInfo = json.extract[UserInfo]

        CommonProcesses.ritualBeforeInsert(user)

        val successInfo: JValue = ("success" -> "OK")

        requestContext.complete(compact(render(successInfo)))
        //				NifiRestCall.nifiCall("http://13.82.55.156:80/contentListener")
        MailService.notifyAdmin()
      }
      case "quickViewVar" => {

        //val v: JValue = Drive.runReport( query, queryParams.values.toList ,report._2 )
        //		println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"+compact(render(v)))
        //		val js = compact(render(v)).toString
        //		val json_str = s"""{\"schema_table_info\":$js}"""
        val quickViewVar: QuickViewVar = json.extract[QuickViewVar]

        val db = quickViewVar.database_name
        val table = quickViewVar.table_name
        var condition = quickViewVar.condition

        //val db = "metam"
        //val table = "core_category"

        val fin_query = "SELECT * from " + db + "." + table + " " + condition + " limit 20"

        //val fin_query = "SELECT * FROM metam.data_services limit 10"
        val fin_map = (rs: WrappedResultSet) => {

          val str = new StringBuilder
          str ++= "{ "
          for (i <- 1 until rs.metaData.getColumnCount) {
            //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
            str ++= "\"" + rs.metaData.getColumnName(i) + "\" : \"" + rs.string(rs.metaData.getColumnName(i)) + "\","
          }
          str ++= "\"" + rs.metaData.getColumnName(rs.metaData.getColumnCount) + "\" : \"" + rs.string(rs.metaData.getColumnName(rs.metaData.getColumnCount)) + "\" }"

          parse(str.toString.replaceAll("\n", " "))
        }
        println("!!!!!!!!!!!!!!!!!!!!!!!working till here!!!!!!!!!!!!!!!")
        val v2: Option[JValue] = Drive.runParamLessHiveReport(fin_query, fin_map, db)

        println(compact(render(v2)))

        v2 match {
          case Some(v) => { requestContext.complete(compact(render(v))) }
          case None => { requestContext.complete(compact(render("{}"))) }
        }

        //        requestContext.complete(compact(render(v2)))

        //println(compact(render(v)))
        //requestContext.complete(compact(render(v)))

      }
      case "updateData" => {

        println("THE POST REQUESTCONTEXT TEST---------------<><><><>" + requestContext.request.entity.asString)

        println("THE POST REQUESTCONTEXT TEST---------------<><><><>" + requestContext.request.entity.asString)

        val updateData: DataServiceUpdate = json.extract[DataServiceUpdate]

        Drive.postRunUpdate(updateData)

        val v: JValue = ("Status" -> "OK")

        //requestContext.complete(StatusCodes.OK)
        requestContext.complete(compact(render(v)))

        MailService.notifyAdmin()
      }
      case "loginPage" => {

        val report = Drive.getReport(key)

        var query = report._1

        println("------>from nisith da---->" + requestContext.request.entity.asString)

        val loginJson: LoginJson = json.extract[LoginJson]

        var user = loginJson.user_name

        var pass = loginJson.password

        if (user == None || pass == None) {
          //val e: JValue = ( "status" -> "-1" )~( "message" -> "Invalid Input" )
          requestContext.complete(compact(render(jsonStat("-1", "Invalid Input"))))
        }

        /*query += " and user_id =" + "\"" + user + "\"" + " AND password = " + "\"" + pass + "\"" + ") a "
        query += """left join (SELECT  id user_id, first_name, middle_name, last_name,
							| user_id user_name,
							| password password,
							| mail_id mail_id,
							| contact_no contact_no,
							| is_admin is_admin,
              | is_subscriber is_subscriber,
              | is_publisher is_publisher
							| FROM users u WHERE 1= 1 and user_id = """.stripMargin + "\"" + user + "\"" + " AND password = " + "\"" + pass + "\"" + ") b on 1=1"*/

        query += " and u.user_id =" + "\"" + user + "\"" + " AND u.PASSWORD = " + "\"" + pass + "\""

        try {

          val uid = Drive.getUidFromUsers(user, pass)

          println("-----------------------UID-------------------------")
          println(uid)
          println("---------------------------------------------------")

          val eihAccessInfosByUid = eihController.getAllEihAccessInfosByUID(uid).asScala.toList

          println("-------------------eihAccessInfosByUid----------------")
          println(eihAccessInfosByUid)
          println("------------------------------------------------------")

          val roleList = eihAccessInfosByUid.foldLeft(List[String]())((c, r) => { val role = Drive.getRoleFromAccessInfoByRoleId(r.getRole_id); c :+ role })

          println("**************roleList: " + roleList)

          val v: JValue = Drive.runParamLessReport(query, report._2)
          println()
          println("------------")
          println(v)
          println("------------")
          println()
          println("*******************JSON BEFORE MERGE: " + compact(render(v)))

          val roleJObject = Drive.getRoleJObject(roleList)

          println("ROLE OBJECT GOT==================" + roleJObject)

          val finalJson = v.children(0) merge roleJObject

          println("-----------just after merge----------")
          println(finalJson)
          println("-------------------------------------")
          // just adding extra JVALUE to FINAL JVALUE REMAINING.
          val loginInfo: LoginInfo = v.extract[LoginInfo]

          println("------------loginINfo--------------")
          println(loginInfo)
          println("-----------------------------------")

          if (loginInfo.user_name == None || loginInfo.password == None) {
            //val e: JValue = ( "status" -> "-1" )~( "message" -> "Invalid Input" )
            requestContext.complete(compact(render(jsonStat("-1", "Invalid Input"))))
          } else {

            Drive.postUpdateUserLastLogin(loginInfo)

            //println("======>"+queryParams.values.mkString("",",", ""))
            println(compact(render(finalJson)))

            requestContext.complete(compact(render(finalJson)))
          }
          /*try {
              val usr = SecurityUtils.getSubject
              val token = new UsernamePasswordToken(user, pass)
              usr.login(token)
              val shiroSessionID = usr.getSession.getId
              setCookie(HttpCookie("SESSION_ID", shiroSessionID.toString())) {
                complete("OK")
                //compact( render( jsonStat("success", usr.getPrincipal.toString ) ) )

              }
            } catch {
              case ex: Exception => {
                println(" Problem while post call resolving " + ex.getMessage)
                ex.printStackTrace
                requestContext.complete(compact(render(jsonStat("failure", "null"))))
              }
            }*/
        }catch{
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case elemEx: NoSuchElementException => {requestContext.complete(compact(render(jsonStat("-1", "User Or Password Does Not Exist"))))}
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }
      case "addUser" => {

        println("Checking ENTITY MSG for ADD USER%%%%%%%%" + entity_msg)

        val newUser: UserEntry = json.extract[UserEntry]
        val roles = newUser.roles
        try {
          Drive.postAddUser(newUser)
          val newlyCreatedUid = Drive.getNewlyCreatedUserId(newUser)
          Drive.insertInfoEihAccessInfoByUid(newlyCreatedUid, roles, eihController)
          val v: JValue = ("Status" -> "OK")

          requestContext.complete(compact(render(v)))
        } catch {
          case sqlEx: SQLException => { requestContext.complete(sqlEx.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }

      }
      case "downloadHistory" => {

        val downloadHistory: DownloadHistory = json.extract[DownloadHistory]

        Drive.postAddHistory(downloadHistory)

        val v: JValue = ("Status" -> "OK")

        requestContext.complete(compact(render(v)))
      }
      case "typeInfo" => {

        val typee: TypeInfo = json.extract[TypeInfo]

        typee.Type match {
          case "S" => {
            val report1 = Drive.getReport("typeS")
            var query1 = report1._1
            query1 += typee.id.toLong

            //val map = ( rs:WrappedResultSet ) =>   { ( "subcategory_id" -> rs.string("subcategory_id") )~( "subcategory_name" -> rs.string("subcategory_name") )~( "category_id" -> rs.string("category_id") )~( "category_name" -> rs.string("category_name") ) }

            val vl1: JValue = Drive.runParamLessReport(query1, report1._2)

            println(compact(render(vl1)))
            requestContext.complete(compact(render(vl1)))
          }

          case "D" => {
            val report2 = Drive.getReport("typeD")
            //            val report2 = Drive.getReport("typeS")
            var query2 = report2._1

            query2 += typee.id.toLong

            //val map = ( rs:WrappedResultSet ) =>   { ( "subcategory_id" -> rs.string("subcategory_id") )~( "subcategory_name" -> rs.string("subcategory_name") )~( "category_id" -> rs.string("category_id") )~( "category_name" -> rs.string("category_name") ) }

            val vl2: JValue = Drive.runParamLessReport(query2, report2._2)

            println(compact(render(vl2)))
            requestContext.complete(compact(render(vl2)))
          }
        }
      }
    }
  }

  def toMap(rs: WrappedResultSet) = {
    val count = rs.metaData.getColumnCount
    val label = rs.metaData.getColumnLabel(1)
    //println("META@@@@@@@@@@DATA@@@@@@@COUNT>>>>>>>>>>>>"+ count+ "----Label----"+ label)
  }

  def processForDownloadData(key: String, subCatId: String, delimiter: String, selectColumns: String) = {
    val report = Drive.getReport(key)

    var query = report._1

    query += " and"

    query += " sub_category = ?"

    val v: JValue = Drive.runQuickViewSingleParamReport(query, subCatId, report._2)
    println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + compact(render(v)) + " And The Selected Columns Are " + selectColumns)

    val js = compact(render(v)).toString
    val json_str = s"""{\"schema_table_info\":$js}"""
    val json = parse(json_str)
    val downloadData: QuickView = json.extract[QuickView]

    val db = downloadData.schema_table_info(0).hive_database_name
    val table = downloadData.schema_table_info(0).hbase_table

    val fin_query = s"SELECT ${selectColumns} from " + db + "." + table

    val fin_map = (rs: WrappedResultSet) => {

      val str = new StringBuilder
      str ++= "{ "
      for (i <- 1 until rs.metaData.getColumnCount) {
        //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
        str ++= "\"" + rs.metaData.getColumnName(i) + "\" : \"" + rs.string(rs.metaData.getColumnName(i)) + "\","
      }
      str ++= "\"" + rs.metaData.getColumnName(rs.metaData.getColumnCount) + "\" : \"" + rs.string(rs.metaData.getColumnName(rs.metaData.getColumnCount)) + "\" }"

      parse(str.toString.replaceAll("\n", " "))
    }
    val v2: Option[JValue] = Drive.runParamLessHiveReport(fin_query, fin_map, db)
    val fileName = table + "_" + new Date().getTime + ".csv"
    val filePath = conf.getString("downloadDataPath.path") + fileName
    val filePathToUpdate = conf.getString("downloadDataPath.pathToUpdate") + fileName
    println("########****************" + filePath)
    val pw = new PrintWriter(filePath)
    v2 match {
      case Some(v) => { pw.write(CommonProcesses.jsonToCsvConverter(compact(render(v)), delimiter)) }
      case None => { pw.write(compact(render("{}"))) }
    }

    pw.close

    Drive.postUpdate(DownloadUrlPath(filePathToUpdate))

  }

  //changing here

  def process(key: String) = {

    println("Checking if key accessible==just after entering process===8888" + key)

    key match {

      case "getAllFromAccessPrivilege" => {
        try {
          val accessPrivileges = appController.getAllFromAccessPrivileges()
          val json = appController.getJsonFromObjectList(accessPrivileges)
          requestContext.complete(json)
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getAllEihAccessInfo" => {
        try {
          val result = eihController.getAllEihAccessInfos()
          val json = eihController.getJsonFromObjectList(result)
          requestContext.complete(json)
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case "getAllAccessInfo" => {
        try {
          val accessInfos = appController.getAllFromAccessInfo()
          val json = appController.getJsonFromObjectList(accessInfos)
          requestContext.complete(json)
        } catch {
          case sqlex: SQLException => { requestContext.complete(sqlex.getMessage) }
          case ex: Exception => { requestContext.complete(ex.getMessage) }
        }
      }

      case _ => {
        val report = Drive.getReport(key)

        var query = report._1

        var fetchUri: String = requestContext.request.message.uri.toString
        println("Checking for the fetchuri here*****" + fetchUri)

        // val p:String = queryParams.values.mkString("",",", "")
        val parameters: List[String] = queryParams.values.toList

        println("HCECKING FOR PARAMETERS length" + parameters.length)

        if (parameters.length == 0) {

          key match {
            case "getGDPRRulesDetails" => {
              query = query + " and a.GDPRID = b.id ORDER BY a.DataType"
            }
            case "getGroupDetails" => {
              query
            }
            case "getFTPDetails" => {
              query
            }
            case "getAdminId" => {
              query
            }
            case "getAllSubjectArea" => {
              query
            }
            case "getFeedbacksInbox" => {
              query
            }
            case "getFeedbacksSentItem" => {
              query
            }
            case "fieldData" => {
              query += " order by id"
            }
            case "barChart" => {
              query += " GROUP BY data_service_name"
            }
            case "getScore" => {
              query
            }
            case "getCount" => {
              query += " GROUP BY subcategory_id,rating"
            }
            case "getUser" => {
              query
            }
            case "getDownloadHistory" => {
              query
            }
            case "pieChart" => {
              query += " GROUP BY Type"
            }
            case "searchField" => {
              query
            }
            case "quickView" => {
              query
            }
            case "downloadData" => {
              query
            }
            case "scoreCard" => {
              query += " order by data_service_id"
            }
            case "selectField" => {
              query += " order by id"
            }
            case "dataServiceData" => {

              query += " order by purchased_date desc"
            }
            case "fieldSubCategory" => {

              query += " order by id"
            }
            case "notificationLog" => {

              query += " order by data_service_id"
            }
            case "adminPage" => {

              query += " order by user_id"
            }
            case "adminPageAlt" => {

              query += " order by user_id"
            }
            case "adminColumnCheck" => {

              query += " order by data_service_id"
            }
          }

        } else if (parameters.length > 0) {

          key match {

            case "getFeedbacksInbox" => {
              query += " and"
              /*for(i <- 0 until parameters.length){
             parameters(i) match{*/
              query += " to_user_id = ?"
              //             }
              //           }

              //           query = " from_user_id = ? or to_user_id = ?"
            }
            case "getFeedbacksSentItem" => {
              query += " and"
              /*for(i <- 0 until parameters.length){
             parameters(i) match{*/
              query += " from_user_id = ?"
              //             }
              //           }

              //           query = " from_user_id = ? or to_user_id = ?"
            }
            case "fieldSubCategory" => {

              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " core_category_id = ? or"
                }
              }
              query += " core_category_id = ?"

              query += " order by id"

            }

            case "getCount" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " subcategory_id = ? or"
                }
              }
              query += " subcategory_id = ?"
              query += " GROUP BY subcategory_id,rating"
            }

            case "getScore" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " subcategory_id = ? or"
                }
              }
              query += " subcategory_id = ?"
            }
            case "barChart" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " service_type  = ? or"
                }
              }
              query += " service_type  = ?"

              query += " GROUP BY data_service_name"
            }
            case "getDownloadHistory" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " B.user_id  = ? or"
                }
              }
              query += " B.user_id  = ?"

            }
            case "pieChart" => {

              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " user_id = ? or"
                }
              }
              query += " user_id = ?"

              query += " GROUP BY Type"
            }

            case "quickView" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " sub_category = ? or"
                }
              }
              query += " sub_category = ?"

            }

            case "downloadData" => {
              query += " and"

              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " sub_category = ? or"
                }
              }
              query += " sub_category = ?"
            }
            case "scoreCard" => {

              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " user_id = ? or"
                }
              }
              query += " user_id = ?"

              query += " order by data_service_id"
            }
            case "getUser" => {
              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " u.id = ? or"
                }
              }
              query += " u.id = ?"
            }
            case "selectField" => {
              query += " and"

              println("+++++++++++++++00000--before")
              val param_val = parameters(parameters.length - 1)
              println("+++++++++++++++00000" + param_val)
              for (i <- 1 to parameters.length - 1) {
                var param_val1 = parameters(i)
                parameters(i) match {
                  case _ => query += " name like '%" + param_val1 + "%' or"
                }
              }
              query += " name like '%" + param_val + "%'"
              //query +=" group by data_element_id"
              query += " order by id"
            }
            case "fieldData" => {
              println("LOOK HERE FOR PARAMETER LENGTH" + parameters.length)

              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " sub_category_id = ? or"
                }
              }
              query += " sub_category_id = ?"
              //query +=" group by data_element_id"
              query += " order by id"
            }
            case "dataServiceData" => {

              println("successfull entry point 2 -------=======---")
              query += " and"

              var flag: Boolean = false
              /*for(i <- 1 to parameters.length - 1){
				parameters(i) match {
					case _ => query += " user_id = ? or"
					}
				}*/
              query += " user_id = ?"

              query += " order by purchased_date desc"
            }
            case "notificationLog" => {

              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " user_id = ? or"
                }
              }
              query += " user_id = ?"

              query += " order by data_service_id"
            }
            case "adminColumnCheck" => {

              query += " and"

              var flag: Boolean = false
              for (i <- 1 to parameters.length - 1) {
                parameters(i) match {
                  case _ => query += " data_service_id = ? or"
                }
              }
              query += " data_service_id = ?"

              query += " order by data_service_id"
            }
          }

        }

        key match {
          case "selectField" => {
            val j: JValue = Drive.runParamLessReport(query, report._2)
            println(compact(render(j)))

            requestContext.complete(compact(render(j)))
          }
          case "downloadData" => {
            val v: JValue = Drive.runQuickViewReport(query, queryParams.values.toList, report._2)
            println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + compact(render(v)))

            val js = compact(render(v)).toString
            val json_str = s"""{\"schema_table_info\":$js}"""
            val json = parse(json_str)
            val downloadData: QuickView = json.extract[QuickView]

            val db = downloadData.schema_table_info(0).hive_database_name
            val table = downloadData.schema_table_info(0).hbase_table

            val fin_query = "SELECT * from " + db + "." + table

            val fin_map = (rs: WrappedResultSet) => {

              val str = new StringBuilder
              str ++= "{ "
              for (i <- 1 until rs.metaData.getColumnCount) {
                //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
                str ++= "\"" + rs.metaData.getColumnName(i) + "\" : \"" + rs.string(rs.metaData.getColumnName(i)) + "\","
              }
              str ++= "\"" + rs.metaData.getColumnName(rs.metaData.getColumnCount) + "\" : \"" + rs.string(rs.metaData.getColumnName(rs.metaData.getColumnCount)) + "\" }"

              parse(str.toString.replaceAll("\n", " "))
            }
            val v2: Option[JValue] = Drive.runParamLessHiveReport(fin_query, fin_map, db)
            val fileName = table + "_" + new Date().getTime + ".csv"
            val filePath = conf.getString("downloadDataPath.path") + fileName
            val filePathToUpdate = conf.getString("downloadDataPath.pathToUpdate") + fileName
            println("########****************" + filePath)
            val pw = new PrintWriter(filePath)
            v2 match {
              case Some(v) => { pw.write(CommonProcesses.jsonToCsvConverter(compact(render(v)), "|")) }
              case None => { pw.write(compact(render("{}"))) }
            }

            pw.close

            Drive.postUpdate(DownloadUrlPath(filePathToUpdate))

            val successInfo: JValue = ("success" -> "OK")

            requestContext.complete(compact(render(successInfo)))
          }
          case "quickView" => {
            val v: JValue = Drive.runQuickViewReport(query, queryParams.values.toList, report._2)
            //val v: JValue = Drive.runReport( query, queryParams.values.toList ,report._2 )
            println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + compact(render(v)))
            val js = compact(render(v)).toString
            val json_str = s"""{\"schema_table_info\":$js}"""
            val json = parse(json_str)
            val quickView: QuickView = json.extract[QuickView]

            val db = quickView.schema_table_info(0).hive_database_name
            val table = quickView.schema_table_info(0).hbase_table

            //val db = "metam"
            //val table = "core_category"

            val fin_query = "SELECT * from " + db + "." + table + " limit 20"

            //val fin_query = "SELECT * FROM metam.data_services limit 10"
            val fin_map = (rs: WrappedResultSet) => {

              val str = new StringBuilder
              str ++= "{ "
              for (i <- 1 until rs.metaData.getColumnCount) {
                //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
                str ++= "\"" + rs.metaData.getColumnName(i) + "\" : \"" + rs.string(rs.metaData.getColumnName(i)) + "\","
              }
              str ++= "\"" + rs.metaData.getColumnName(rs.metaData.getColumnCount) + "\" : \"" + rs.string(rs.metaData.getColumnName(rs.metaData.getColumnCount)) + "\" }"

              parse(str.toString.replaceAll("\n", " "))
            }
            val v2: Option[JValue] = Drive.runParamLessHiveReport(fin_query, fin_map, db)
            //		val v2: JValue = Drive.runParamLessReport(fin_query, fin_map)
            //		println(compact(render(v2)))

            v2 match {
              case Some(v) => { requestContext.complete(compact(render(v))) }
              case None => { requestContext.complete(compact(render("{}"))) }
            }
            //        requestContext.complete(compact(render(v2)))

            //println(compact(render(v)))
            //requestContext.complete(compact(render(v)))

          }
          case "getAllSubjectArea" => {
            val v: List[JValue] = Drive.runSubjectAreaReport(query, queryParams.values.toList, report._2)

            val finalJson = CommonProcesses.getFinalJsonWithSchemaAndColumnName(v)

            println(finalJson)

            requestContext.complete(finalJson)
          }
          case _ => {

            //val v :JValue = Drive.runReport(report._1, queryParams.values.toList ,report._2)
            val v: JValue = Drive.runReport(query, queryParams.values.toList, report._2)

            println(compact(render(v)))

            requestContext.complete(compact(render(v)))
          }
        }
      }
    }

  }
}
