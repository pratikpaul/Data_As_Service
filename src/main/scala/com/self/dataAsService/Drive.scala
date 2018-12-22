package com.self.dataAsService

import java.sql._
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Serialization.write
import com.typesafe.config.ConfigFactory
import scalikejdbc._
import scalikejdbc.WrappedResultSet
import sys.process._
import com.self.dataAsService.util.CommonProcesses
import org.json4s.native.Serialization.write
import com.self.dataAsService.util.CustomNoneJNullSerializer
import com.self.eihDrive.EihController

object Drive {
  implicit val formats = DefaultFormats + new CustomNoneJNullSerializer()
  var connection: Connection = _
  var callableStatement: CallableStatement = _
  var statement: Statement = _
  var rs: ResultSet = _
  val conf = ConfigFactory.load

  import DatabaseConn._

  def getJsonFromObj[T](i: T): String = compact(render(Extraction.decompose(i)))
  def getJsonFromObjList[T](i: T): String = compact(render(Extraction.decompose(i)))

  case class Report(val sql: String, val f: (WrappedResultSet => JValue)) {}
  val registry = scala.collection.mutable.Map[String, Report]()
  //changed here
  val postRegistry = scala.collection.mutable.Map[String, String]()
  //change done

  def register(key: String, sql: String, f: (WrappedResultSet => JValue)) = {

    registry += (key -> new Report(sql, f))
  }

  //changed here
  def registerForPost(key: String) = {

    postRegistry += (key -> "checkPoint_1")
  }

  def getReportForPost(key: String): String = {
    val r = postRegistry(key)
    r
  }
  //change done

  def getReport(key: String): (String, (WrappedResultSet => JValue)) = {

    val r = registry(key)
    (r.sql, r.f);
  }

  def getRoleJObject(roles: List[String]): JValue = {
    val ser = write(roles)
    val jsonStr = s"""{"roles" : $ser}"""
    val jsonVal = parse(jsonStr)
    jsonVal
  }

  def getRoleIdFromAccessInfoById(id: Int): Int = {
    (sql"""select role_id from ACCESS_INFO where id = $id""".map(rs => rs.int("role_id")).single.apply()).get
  }

  def getRoleFromAccessInfoByRoleId(roleId: Int): String = {
    val role = (sql"""select role from ACCESS_INFO where role_id = $roleId""".map(rs => rs.string("role")).single.apply()).get
    role
  }

  def getUidFromUsers(user: String, pass: String): Int = {
    val uid = (sql"""select id from users where user_id=$user and password=$pass""".map(rs => rs.int("id")).single.apply()).get
    uid
  }

  def deleteByQueryId(deleteByQueryId: DeleteByQueryId) {
    val queryId = deleteByQueryId.queryId

    sql"""DELETE FROM user_queries WHERE query_id=$queryId;""".execute.apply()
  }

  def isGDPRPresent(subjectAreaId: Int, DatasetId: Int): Boolean = {
    val check = (sql"""select exists(select * from GDPR_Column_Map where subjectareaID = ${subjectAreaId} and DatasetID = ${DatasetId}) ifExists""".map(rs => rs.int("ifExists")).single.apply()).get
    check match {
      case 0 => return false
      case 1 => return true
    }
  }

  def getIdfromSubAreaAndDataSet(subjectAreaId: Int, DatasetId: Int): List[Int] = {
    sql"""select Id id from GDPR_Column_Map where subjectareaID = ${subjectAreaId} and DatasetID = ${DatasetId}""".map(rs => rs.int("id")).list.apply()
  }

  def saveGDPRColumnMap(in: GDPRColumnMap) = {

    val subjectareaId = in.subjectareaID
    val subjectareaName = in.subjectareaName
    val datasetId = in.datasetID
    val datasetName = in.datasetName
    val gdprRuleArray = in.gdprRuleArray
    val createdBy = in.createdBy

    gdprRuleArray.foreach(f => {
      val columnId = f.columnID
      val columnName = f.columnName
      val isVisible = f.isVisible
      val gdprRuleId = f.GDPRRuleID
      val gdprRuleName = f.GDPRRuleName

      sql"""INSERT INTO GDPR_Column_Map(subjectareaID,subjectareaName,DatasetID,DatasetName,ColumnID,ColumnName,IsVisible,GDPRRuleID,GDPRRuleName,CreatedBy,CreationDate) VALUES (${subjectareaId}, ${subjectareaName},${datasetId},${datasetName},${columnId},${columnName},${isVisible},${gdprRuleId},${gdprRuleName},${createdBy},NOW(6))""".update.apply()
    })

  }

  def updateGDPRColumnMap(ids: List[Int], user: String) = {
    ids.foreach { id =>
      sql"""UPDATE GDPR_Column_Map SET MarkDelete=1, ModifiedBy=${user}, ModificationDate=NOW(6) WHERE Id=${id}""".update.apply()
    }

  }

  def postDeleteFtpDetails(idInfo: MapIds) = {
    val id = idInfo.id

    sql"""DELETE FROM ftp_master WHERE ID = $id""".execute.apply()
  }

  def postUpdateSubscribeCartMarkRead(cartInfo: MapIds) = {
    val id = cartInfo.id

    sql"""UPDATE subscribeCart SET Mark_Read=1 WHERE id=${id}""".update.apply()
  }

  def getCoreCategoryIdByName(name: String): Int = {
    (sql"""select id from core_category where name = $name""".map(rs => rs.int("id")).single.apply()).get
  }

  def getCoreSubCategoryIdByName(name: String): Int = {
    (sql"""select id from core_subcategory where name = $name""".map(rs => rs.int("id")).single.apply()).get
  }

  def getUserIdByUser(user: String): Int = {
    (sql"""select id from users where user_id = $user""".map(rs => rs.int("id")).single.apply()).get
  }

  def postUpdateUserLastLogin(loginInfo: LoginInfo) = {
    val username = loginInfo.user_name
    val password = loginInfo.password

    sql"""UPDATE users SET last_login=NOW(6) WHERE user_id=${username} AND PASSWORD=${password}""".update.apply()
  }

  def insertIntoDownloadDetails(downloadDetails: DownloadDetails) = {
    val categoryId = downloadDetails.categoryId
    val subCategoryId = downloadDetails.subCategoryId
    val userId = downloadDetails.userId
    val downloadType = downloadDetails.downloadType
    val downloadStatus = downloadDetails.downloadStatus

    sql"""INSERT INTO download_details(category_id, sub_category_id, user_id, download_type, download_date, download_status) VALUES ($categoryId, $subCategoryId, $userId, $downloadType, NOW(6), $downloadStatus)""".update.apply()
  }

  def postEditFtpDetails(ftpDetails: FTPDetails) = {
    val id = ftpDetails.id
    val ftpName = ftpDetails.ftp_name
    val ftpServer = ftpDetails.ftp_server
    val ftpUserName = ftpDetails.ftp_user_name
    val ftpPassword = ftpDetails.ftp_password
    val ftpPort = ftpDetails.ftp_port
    val ftpDefaultLocation = ftpDetails.ftp_default_location

    sql"""UPDATE ftp_master SET FTP_Name = $ftpName, FTP_Server = $ftpServer, FTP_User_Name = $ftpUserName, FTP_Password = $ftpPassword, FTP_Port = $ftpPort, FTP_Default_Location = $ftpDefaultLocation WHERE ID = $id""".update.apply()
  }

  def postSaveFtpDetails(ftpDetails: FTPDetails) = {
    val ftpName = ftpDetails.ftp_name
    val ftpServer = ftpDetails.ftp_server
    val ftpUserName = ftpDetails.ftp_user_name
    val ftpPassword = ftpDetails.ftp_password
    val ftpPort = ftpDetails.ftp_port
    val ftpDefaultLocation = ftpDetails.ftp_default_location

    sql"""INSERT INTO ftp_master(FTP_Name, FTP_Server, FTP_User_Name, FTP_Password, FTP_Port, FTP_Default_Location) VALUES ($ftpName, $ftpServer, $ftpUserName, $ftpPassword, $ftpPort, $ftpDefaultLocation)""".update.apply()
  }

  def postAddHistory(downloadHistory: DownloadHistory) = {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))

    implicit val session = AutoSession*/

    val user_id = downloadHistory.user_id.toLong
    val data_service_id = downloadHistory.data_service_id.toLong
    val download_details = downloadHistory.download_details
    val download_status = downloadHistory.download_status

    sql"""insert into order_download_history ( user_id, data_service_id, download_details, download_status, created_date ) values ( $user_id, $data_service_id, $download_details, $download_status, Current_Date)""".update.apply()
    //    ConnectionPool.close(ds)
  }

  def postMapSubjectAreaToGroup(mapSubjectAreaToGroup: MapSubjectAreaToGroup) = {
    val subCategoryId = mapSubjectAreaToGroup.subCategoryId
    val createdBy = mapSubjectAreaToGroup.createdBy
    val groupIds = mapSubjectAreaToGroup.groupIds

    sql"""UPDATE groupSubjectAreaMap SET MarkDelete=1, ModifiedBy=${createdBy}, ModificationDate=NOW(6) where SubCategoryId = ${subCategoryId}""".update.apply

    groupIds.foreach(x => sql"""INSERT INTO groupSubjectAreaMap(SubCategoryId, GroupId, CreatedBy, ModifiedBy, ModificationDate) VALUES (${subCategoryId}, ${x.id}, ${createdBy}, NULL, NULL)""".update.apply)

  }

  def postMapSubjectAreaListToGroup(mapSubjectAreaListToGroup: MapSubjectAreaListToGroup) = {
    val subCategoryIds = mapSubjectAreaListToGroup.subCategoryIds
    val createdBy = mapSubjectAreaListToGroup.createdBy
    val groupIds = mapSubjectAreaListToGroup.groupIds

    subCategoryIds.foreach(subCategoryId => {
      sql"""UPDATE groupSubjectAreaMap SET MarkDelete=1, ModifiedBy=${createdBy}, ModificationDate=NOW(6) where SubCategoryId = ${subCategoryId}""".update.apply

      groupIds.foreach(x => sql"""INSERT INTO groupSubjectAreaMap(SubCategoryId, GroupId, CreatedBy, ModifiedBy, ModificationDate) VALUES (${subCategoryId}, ${x.id}, ${createdBy}, NULL, NULL)""".update.apply)
    })

  }

  def postMapGroupToSubjectArea(mapGroupToSubjectArea: MapGroupToSubjectArea) = {
    val groupId = mapGroupToSubjectArea.groupId
    val createdBy = mapGroupToSubjectArea.createdBy
    val subCategoryIds = mapGroupToSubjectArea.subCategoryIds

    sql"""UPDATE groupSubjectAreaMap SET MarkDelete=1, ModifiedBy=${createdBy}, ModificationDate=NOW(6) where GroupId = ${groupId}""".update.apply

    subCategoryIds.foreach(x => sql"""INSERT INTO groupSubjectAreaMap(SubCategoryId, GroupId, CreatedBy, ModifiedBy, ModificationDate) VALUES (${x.id}, ${groupId}, ${createdBy}, NULL, NULL)""".update.apply)

  }

  def postDeleteFromCart(mapId: MapIds) = {
    val userId = mapId.id

    sql"""UPDATE subscribeCart SET MarkDelete = 1 WHERE id = ${userId}""".update.apply()
  }

  def postMapGroupToUser(mapGroupToUser: MapGroupToUser) = {
    val userId = mapGroupToUser.userId
    val createdBy = mapGroupToUser.createdBy
    val groupIds = mapGroupToUser.groupIds

    sql"""UPDATE groupUserMap SET MarkDelete=1, ModifiedBy=${createdBy}, ModificationDate=NOW(6) where UserId = ${userId}""".update.apply

    groupIds.foreach(x => sql"""INSERT INTO groupUserMap(UserId, GroupId, CreatedBy, ModifiedBy, ModificationDate) VALUES (${userId}, ${x.id}, ${createdBy}, NULL, NULL)""".update.apply)
  }

  def postAdminDatasetDetails(spec: MapIds): List[CoreCatJoinCoreSub] = {
    val catId = spec.id
    val avg = (l: List[Int]) => l.length match {
      case x if x > 0 => { l.foldLeft(0)(_ + _) / l.length }
      case _ => 0
    }
    /* val isAdmin = spec.isAdmin
    val isPub = spec.isPublisher
    val isSub = spec.isSubscriber*/

    val core_category_name = (sql"""SELECT name FROM core_category WHERE id = ${catId}""".map(rs => rs.string("name")).single.apply()).get
    val coreCatJoinCoreSubRs = sql"""SELECT cs.id core_subcategory_id, cs.name core_subcategory_name FROM core_category c JOIN core_subcategory cs ON c.id = cs.core_category_id WHERE c.id = ${catId}""".map(rs => { (rs.int("core_subcategory_id"), rs.string("core_subcategory_name")) }).list.apply()

    val coreCatJoinCoreSub = coreCatJoinCoreSubRs.foldLeft(List[CoreCatJoinCoreSub]())((c, r) => {

      val pubCount = getPublisherCountBySubCatId(r._1)
      val subscriberCount = getSubcriberCountBySubCatId(r._1)
      val downloadCount = getDownloadCountBySubCatId(r._1)
      val gdprCount = getGDPRCount(r._1)
      var coreSubCatJoinGroupSubjAreaMap: List[CoreSubCatJoinGroupSubjAreaMap] = List()
      val leaves = sql"""select gsa.groupId group_id, tg.groupname group_name from core_subcategory cs join groupSubjectAreaMap gsa on cs.id = gsa.SubCategoryId join tGroup tg on gsa.groupId = tg.id where cs.id = ${r._1} AND gsa.MarkDelete = 0 AND tg.id in (select id from tGroup where id not in (select parentid from tGroup where MarkDelete = 0))""".map(rs => rs.int("group_id")).list.apply()

      leaves.foreach(f => {
        val bottomUpGrpHierList = sql"""SELECT  @r AS group_id,(SELECT  @r := parentid FROM tGroup WHERE  id = group_id and markdelete=0) parent,(SELECT  @r1 := groupname FROM tGroup WHERE  id = group_id and markdelete=0 ) group_name, @l := @l + 1 AS lvl FROM (SELECT  @r := ${f},@l := 0,@cl := 0) vars, tGroup h WHERE    @r <> 0""".map(rs => rs.string("group_name")).list.apply().filter(x => x != null)
        val bottomUpGrpLstReverse = bottomUpGrpHierList.reverse
        var bottomUpGrpPath = bottomUpGrpLstReverse.foldLeft("")(_ + ">" + _)
        bottomUpGrpPath = bottomUpGrpPath.drop(1)
        coreSubCatJoinGroupSubjAreaMap = coreSubCatJoinGroupSubjAreaMap :+ CoreSubCatJoinGroupSubjAreaMap(f, bottomUpGrpPath)
      })

      //      val coreSubCatJoinGroupSubjAreaMap = sql"""select gsa.groupId group_id, tg.groupname group_name from core_subcategory cs join groupSubjectAreaMap gsa on cs.id = gsa.SubCategoryId join tGroup tg on gsa.groupId = tg.id where cs.id = ${r._1} AND gsa.MarkDelete = 0""".map(rs => CoreSubCatJoinGroupSubjAreaMap(rs.int("group_id"), rs.string("group_name"))).list.apply()

      val coreSubCatJoinPublishedSubj = sql"""select ps.id published_subjects_id, ps.Status status, ps.Visibility visibility, ps.Summary summary, ps.Description description, ps.Important_Columns important_columns, ps.Unit_Price unit_price, ps.Unit_Currency unit_currency, ps.CreatedBy createdBy, ps.CreationDate creationDate from core_subcategory cs join published_subjects ps on cs.id = ps.core_subcategory_id where cs.id = ${r._1}""".map(rs => CoreSubCatJoinPublishedSubj(rs.int("published_subjects_id"), rs.string("status"), rs.boolean("visibility"), rs.string("summary"), rs.string("description"), rs.string("important_columns"), rs.double("unit_price"), rs.string("unit_currency"), rs.string("createdBy"), rs.string("creationDate"))).list.apply()

      val rating = sql"""SELECT rating FROM user_rating WHERE subcategory_id = ${r._1}""".map(rs => rs.int("rating")).list.apply()

      c :+ CoreCatJoinCoreSub(r._1, r._2, avg(rating), coreSubCatJoinGroupSubjAreaMap, coreSubCatJoinPublishedSubj, pubCount, subscriberCount, downloadCount, gdprCount)
    })

    coreCatJoinCoreSub
  }

  def postSearchByCatAndSubCatName(uidIsAdminSearch: UidIsAdminSearch): List[DataCatalogInfos] = {
    val userId = uidIsAdminSearch.userId
    val isAdmin = uidIsAdminSearch.isAdmin
    val searchTerm = uidIsAdminSearch.searchTerm
    val showUnmapped = uidIsAdminSearch.showUnmapped

    val catIdCnt = getMappedDatasetCountAndCatId(userId, isAdmin, Some(searchTerm))
    val results = getDataCatalogInfoList(catIdCnt, isAdmin, Some(showUnmapped))

    results
  }

  def postCompactSubjectAreaInfo(uidIsAdmin: UidIsAdmin): List[DataCatalogInfos] = {
    val userId = uidIsAdmin.userId
    val isAdmin = uidIsAdmin.isAdmin

    val catIdCnt = getMappedDatasetCountAndCatId(userId, isAdmin)
    val dataCatlaogInfos = getDataCatalogInfoList(catIdCnt, isAdmin)

    dataCatlaogInfos
  }

  def getDataCatalogInfoList(catIdCnt: List[CatIdAndCount], isAdmin: Boolean, showUnmapped: Option[Boolean] = None): List[DataCatalogInfos] = {

    var dataCatlaogInfos: List[DataCatalogInfos] = List()
    val unmappedDatasetCount = (a: Int, b: Int) => a - b

    showUnmapped match {
      case None => {
        catIdCnt.foreach(x => {
          val mappedDatsetCount = x.count
          val catId = x.catId
          val catName = x.catName
          val pubCount = getPublisherCountByCatId(catId)
          val subscriberCount = getSubcriberCountByCatId(catId)
          val downloadCount = getDownloadCountByCatId(catId)
          isAdmin match {
            case true => { dataCatlaogInfos = dataCatlaogInfos :+ DataCatalogInfos(catId, catName, mappedDatsetCount, unmappedDatasetCount(mappedDatsetCount, getAdminMappedCount(catId)), pubCount, subscriberCount, downloadCount) }
            case false => { dataCatlaogInfos = dataCatlaogInfos :+ DataCatalogInfos(catId, catName, mappedDatsetCount, unmappedDatasetCount(getTotalDatasetByCatId(catId), mappedDatsetCount), pubCount, subscriberCount, downloadCount) }
          }
        })
      }

      case Some(v) => {
        catIdCnt.foreach(x => {
          val mappedDatsetCount = x.count
          val isUnmapped = (a: Int, b: Int) => (a - b) > 0
          val catId = x.catId
          val catName = x.catName
          val pubCount = getPublisherCountByCatId(catId)
          val subscriberCount = getSubcriberCountByCatId(catId)
          val downloadCount = getDownloadCountByCatId(catId)
          /*val subCatId = x.subCatId
      val subCatName = x.subCatName*/

          v match {
            case true => {
              isUnmapped(mappedDatsetCount, getAdminMappedCount(catId)) match {
                case true => { dataCatlaogInfos = dataCatlaogInfos :+ DataCatalogInfos(catId, catName, mappedDatsetCount, unmappedDatasetCount(mappedDatsetCount, getAdminMappedCount(catId)), pubCount, subscriberCount, downloadCount) }
                case _ => {}
              }
            }
            case false => {
              dataCatlaogInfos = dataCatlaogInfos :+ DataCatalogInfos(catId, catName, mappedDatsetCount, unmappedDatasetCount(mappedDatsetCount, getAdminMappedCount(catId)), pubCount, subscriberCount, downloadCount)
            }
          }
        })
      }
    }

    dataCatlaogInfos
  }

  def getDownloadCountByCatId(catId: Int): Int = {
    val count = DB readOnly { implicit session => sql"""select count(id) download_count from download_details where category_id = ${catId}""".map(rs => rs.int("download_count")).single.apply() }

    count.get
  }

  def getDownloadCountBySubCatId(subCatId: Int): Int = {
    val count = DB readOnly { implicit session => sql"""select count(id) download_count from download_details where sub_category_id = ${subCatId}""".map(rs => rs.int("download_count")).single.apply() }

    count.get
  }

  def getGDPRCount(datasetId: Int): Int = {
    val count = sql"""select count(id) gdpr_count from GDPR_Column_Map where DatasetID = ${datasetId} and MarkDelete = 0""".map(rs => rs.int("gdpr_count")).single.apply()
    count.get
  }

  def getMappedDatasetCountAndCatId(userId: Int, isAdmin: Boolean, term: Option[String] = None): List[CatIdAndCount] = {

    var catIdCountTuple: List[CatIdAndCount] = List()

    term match {
      case None => {
        isAdmin match {
          case true => { catIdCountTuple = sql"""SELECT z.catname catname, z.catid cat_id, COUNT(*) COUNT FROM (SELECT c.id catid,c.name catname,cs.id, cs.name FROM core_category c JOIN core_subcategory cs ON c.id = cs.core_category_id) z GROUP BY z.catid""".map(rs => CatIdAndCount(rs.string("catname"), rs.int("cat_id"), rs.int("count"))).list.apply() }
          case false => { catIdCountTuple = sql"""select z.catname catname, z.catid cat_id, count(*) count from (select c.id catid,c.name catname,cs.id, cs.name from core_category c join core_subcategory cs on c.id = cs.core_category_id where cs.id in (select SubCategoryId from groupSubjectAreaMap where GroupId in (select GroupId from groupUserMap where UserId= ${userId}))) z group by z.catid""".map(rs => CatIdAndCount(rs.string("catname"), rs.int("cat_id"), rs.int("count"))).list.apply() }
        }
      }
      case Some(v) => {
        val searchTerm = "%" + v + "%"
        isAdmin match {
          case true => { catIdCountTuple = sql"""select z.catname catname, z.catid cat_id, z.subcatname subcatname, count(*) count from (select c.id catid,c.name catname,cs.id subcatid, cs.name subcatname from core_category c join core_subcategory cs on c.id = cs.core_category_id) z group by z.catid having z.catname like $searchTerm or z.subcatname like $searchTerm""".map(rs => CatIdAndCount(rs.string("catname"), rs.int("cat_id"), rs.int("count"))).list.apply() }
          case false => { catIdCountTuple = sql"""select z.catname catname, z.catid cat_id, z.subcatname subcatname, count(*) count from (select c.id catid,c.name catname,cs.id subcatid, cs.name subcatname from core_category c join core_subcategory cs on c.id = cs.core_category_id where cs.id in (select SubCategoryId from groupSubjectAreaMap where GroupId in (select GroupId from groupUserMap where UserId= ${userId}))) z group by z.catid having z.catname like $searchTerm or z.subcatname like $searchTerm""".map(rs => CatIdAndCount(rs.string("catname"), rs.int("cat_id"), rs.int("count"))).list.apply() }
        }
      }
    }

    catIdCountTuple
  }

  def getAdminMappedCount(catId: Int): Int = {
    val count = DB readOnly {
      implicit session => sql"""SELECT COUNT(*) count FROM (SELECT DISTINCT SubCategoryId FROM groupSubjectAreaMap WHERE SubCategoryId IN (SELECT id FROM core_subcategory WHERE core_category_id = ${catId})) z""".map(rs => rs.int("count")).single.apply()
    }

    count.get
  }

  def getPublisherCountByCatId(catId: Int): Int = {
    val count = DB readOnly {
      implicit session => sql"""select count(id) pubCount from users where id in (select distinct  userId from groupUserMap where groupId in (select distinct(groupID) from publishedDatasetGroupMapping where publishID in (select id from published_subjects where core_category_id = ${catId}))) AND is_publisher = 1""".map(rs => rs.int("pubCount")).single.apply()
    }

    count.get
  }

  def getPublisherCountBySubCatId(subCatId: Int): Int = {
    val count = DB readOnly {
      implicit session => sql"""select count(id) pubCount from users where id in (select distinct  userId from groupUserMap where groupId in (select distinct(groupID) from publishedDatasetGroupMapping where publishID in (select id from published_subjects where core_subcategory_id = ${subCatId}))) AND is_publisher = 1""".map(rs => rs.int("pubCount")).single.apply()
    }

    count.get
  }

  def getSubcriberCountByCatId(catId: Int): Int = {
    val count = DB readOnly {
      implicit session => sql"""select count(id) subscriberCount from users where id in (select distinct  userId from groupUserMap where groupId in (select distinct(groupID) from publishedDatasetGroupMapping where publishID in (select id from published_subjects where core_category_id = ${catId}))) AND is_subscriber = 1""".map(rs => rs.int("subscriberCount")).single.apply()
    }

    count.get
  }

  def getSubcriberCountBySubCatId(subCatId: Int): Int = {
    val count = DB readOnly {
      implicit session => sql"""select count(id) subscriberCount from users where id in (select distinct  userId from groupUserMap where groupId in (select distinct(groupID) from publishedDatasetGroupMapping where publishID in (select id from published_subjects where core_subcategory_id = ${subCatId}))) AND is_subscriber = 1""".map(rs => rs.int("subscriberCount")).single.apply()
    }

    count.get
  }

  def getUnmappedDatasetCount(totalCount: Int, mappedCount: Int): Int = {
    val result = totalCount - mappedCount
    result
  }

  def getTotalDatasetByCatId(catId: Int): Int = {
    val totalDatasetCnt = (DB readOnly { implicit session => sql"""select count(*) count from (select  c.id catid,c.name catname,cs.id, cs.name from core_category c join core_subcategory cs on c.id = cs.core_category_id) z  group by z.catid having z.catid = ${catId};""".map(rs => (rs.int("count"))).single.apply() }).get
    totalDatasetCnt
  }

  def postUserGroupDelete(groupUserDelete: GroupUserDelete) = {
    val groupId = groupUserDelete.groupId
    val modifiedBy = groupUserDelete.user

    val groupUserMapRows = getGroupUserMapRows(groupId)
    val groupSubjectAreaMapRows = getGroupSubjectAreaMapRows(groupId)

    sql"""UPDATE tGroup SET MarkDelete=1, ModifiedBy=${groupUserDelete.user}, ModificationDate=NOW(6) WHERE id = ${groupId}""".update.apply

    if (groupUserMapRows.length > 0) {
      sql"""UPDATE groupUserMap SET MarkDelete=1, ModifiedBy=${groupUserDelete.user}, ModificationDate=NOW(6) where GroupId = ${groupId}""".update.apply
      groupUserMapRows.foreach { x =>
        {
          sql"""INSERT INTO groupUserMap(UserId, GroupId, CreatedBy, CreationDate, ModifiedBy, ModificationDate) VALUES (${x.userId}, (SELECT ParentId FROM tGroup WHERE Id=${groupId}), ${x.createdBy}, ${x.creationDate}, ${modifiedBy}, NOW(6))""".update.apply
          //retrieving the child ids of the parent and making the insert for them as well
          val childIds = DB readOnly { implicit session =>
            sql"""SELECT id FROM (SELECT * FROM tGroup WHERE markdelete=0 ORDER BY parentid, id) group_sorted,(SELECT @pv := (SELECT ParentId FROM tGroup WHERE Id=${groupId})) initialisation WHERE find_in_set(parentid, @pv) > 0 AND @pv := concat(@pv, ',', id)""".map(rs => rs.int("id")).list.apply()
          }
          childIds.foreach(f => { sql"""INSERT INTO groupUserMap(UserId, GroupId, CreatedBy, CreationDate, ModifiedBy, ModificationDate) VALUES (${x.userId}, ${f}, ${x.createdBy}, ${x.creationDate}, ${modifiedBy}, NOW(6))""".update.apply })
        }
      }
    }
    if (groupSubjectAreaMapRows.length > 0) {
      sql"""UPDATE groupSubjectAreaMap SET MarkDelete=1, ModifiedBy=${groupUserDelete.user}, ModificationDate=NOW(6) where GroupId = ${groupId}""".update.apply
      groupSubjectAreaMapRows.foreach { x =>
        {
          sql"""INSERT INTO groupSubjectAreaMap(SubCategoryId, GroupId, CreatedBy, CreationDate, ModifiedBy, ModificationDate) VALUES (${x.subCategoryId}, (SELECT ParentId FROM tGroup WHERE Id=${groupId}), ${x.createdBy}, ${x.creationDate}, ${modifiedBy}, NOW(6))""".update.apply
          //retrieving the child ids of the parent and making the insert for them as well
          val childIds = DB readOnly { implicit session =>
            sql"""SELECT id FROM (SELECT * FROM tGroup WHERE markdelete=0 ORDER BY parentid, id) group_sorted,(SELECT @pv := (SELECT ParentId FROM tGroup WHERE Id=${groupId})) initialisation WHERE find_in_set(parentid, @pv) > 0 AND @pv := concat(@pv, ',', id)""".map(rs => rs.int("id")).list.apply()
          }
          childIds.foreach(f => { sql"""INSERT INTO groupSubjectAreaMap(SubCategoryId, GroupId, CreatedBy, CreationDate, ModifiedBy, ModificationDate) VALUES (${x.subCategoryId}, ${f}, ${x.createdBy}, ${x.creationDate}, ${modifiedBy}, NOW(6))""".update.apply })
        }
      }
    }
  }

  def getGroupUserMapRows(groupId: Int): List[GroupUserMapTable] = {
    val report = getReport("groupUserMapDetails")
    var query = report._1 + s" and GroupId=${groupId} and MarkDelete=0"
    val v = runGeneralQuery(query, report._2)
    v.foldLeft(List[GroupUserMapTable]())((c, r) => { c :+ r.extract[GroupUserMapTable] })
  }

  def getGroupSubjectAreaMapRows(groupId: Int): List[GroupUserSubjectAreaTable] = {
    val report = getReport("groupSubjectAreaMapDetails")
    var query = report._1 + s" and GroupId=${groupId} and MarkDelete=0"
    val v = runGeneralQuery(query, report._2)
    v.foldLeft(List[GroupUserSubjectAreaTable]())((c, r) => { c :+ r.extract[GroupUserSubjectAreaTable] })
  }

  def postRenameGroupName(groupRename: InputIntAndString) = {
    val id = groupRename.id
    val newName = groupRename.info

    sql"""UPDATE tGroup SET GroupName=${newName} WHERE id = ${id}""".update.apply
  }

  def postInsertIntoGroup(groupInsert: GroupInsert) = {

    groupInsert.parentName match {
      case x if x != "1" => {
        sql"""INSERT INTO tGroup ( GroupName, ParentId, CreatedBy, ModificationDate ) VALUES (${groupInsert.groupName}, (SELECT Id FROM tGroup t WHERE t.GroupName = ${groupInsert.parentName} and MarkDelete = 0), ${groupInsert.createdBy}, NULL)""".update.apply
      }
      case _ => {
        val parentId = 1
        sql"""INSERT INTO tGroup ( GroupName, ParentId, CreatedBy, ModificationDate ) VALUES (${groupInsert.groupName}, ${parentId}, ${groupInsert.createdBy}, NULL)""".update.apply
      }
    }
  }

  def postTestRule(testRule: TestRule): String = {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/

    val query = s"""SELECT hive_database_name, hbase_table FROM metadata_core WHERE sub_category = ${testRule.subCategoryId}"""
    val report = getReport("quickView")
    val v: JValue = render(runGeneralQuery(query, report._2))

    val js = compact(render(v)).toString
    val json_str = s"""{\"schema_table_info\":$js}"""
    val json = parse(json_str)
    val quickView: QuickView = json.extract[QuickView]

    val db = quickView.schema_table_info(0).hive_database_name
    val table = quickView.schema_table_info(0).hbase_table

    //val db = "metam"
    //val table = "core_category"

    var fin_query = "SELECT * from " + db + "." + table + " where " //+ " limit 10"

    fin_query = testRule.filterCondition.foldLeft(fin_query) { (c, r) =>
      r.condition match {
        case "like" => { c + r.parentheses_1 + r.selectedColumn + " " + r.condition + " '%" + r.value + "%' " + r.relationaloperator + r.parentheses_2 + " " }
        case _ => {
          r.value.isInstanceOf[String] match {
            case true => { c + r.parentheses_1 + r.selectedColumn + " " + r.condition + "'" + r.value + "' " + r.relationaloperator + r.parentheses_2 + " " }
            case false => { c + r.parentheses_1 + r.selectedColumn + " " + r.condition + r.value + " " + r.relationaloperator + r.parentheses_2 + " " }
          }
        }
      }
    }

    println()
    println()
    println("FINQUERY ====> " + fin_query)
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

    var result = ""
    v2 match {
      case Some(v) => { result = compact(render(v)) }
      case None => { result = "{}" }
    }
    result
  }

  def approveOrReject(approveInfos: List[ApproveOrReject]) = {

    approveInfos.foreach(approveInfo => {
      sql"""UPDATE subscribeCart SET ApproveStatus = ${approveInfo.approveOrRejectStatus}, ApproverOrRejectorID = ${approveInfo.userId}, ApprovedOrRejectBy = ${approveInfo.userName}, ApproveOrRejectDate = NOW(6), ApproveOrRejectComment = ${approveInfo.comment} WHERE id = ${approveInfo.subscribeCartId}""".update.apply()

      approveInfo.approveOrRejectStatus match {
        case 1 => {
          val report = Drive.getReport("getFromCartByIdForSubscriber2")
          val query = report._1 + s" AND id = ${approveInfo.subscribeCartId}"
          val row = render(Drive.runGeneralQuery(query, report._2))

          val subscribeCart = row.extract[SubscribeCartAllString]

          println("**********SUBSCRIBE CART ====" + subscribeCart)
          val selectedColumnString = subscribeCart.selected_columns
          println("SELECTED COLS STRING==>" + selectedColumnString)
          val selectedColumnJson = parse(selectedColumnString)
          println("SELECTED COLS JSON ==>" + selectedColumnJson)
          val selectedColumns = selectedColumnJson.extract[List[Selected_columns]]
          println("SELECTED COLS OBJECT ===> " + selectedColumns)

          //    val userinfo = UserInfo(${subscribeCart.id},)

          val isDownloadUrl = subscribeCart.shareType match {
            case "url" => { true }
            case _ => { false }
          }

          val ftp_username = approveInfo.ftp_username match {
            case "" => { None }
            case x: String => { Some(x) }
          }
          val ftp_password = approveInfo.ftp_password match {
            case "" => { None }
            case x: String => { Some(x) }
          }
          val filterCond = approveInfo.filterCond

          val dateValue = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

          val serviceDetails = ServiceDetail(subscribeCart.core_category_id.toString, subscribeCart.core_subcategory_id.toString, subscribeCart.metadata_core_id.toString, Some("1"), subscribeCart.serviceType, dateValue, subscribeCart.row_delimeter, subscribeCart.column_delimeter, Some(subscribeCart.is_encrypted.toString), Some(subscribeCart.is_header.toString), None, approveInfo.data_service_name, subscribeCart.shareType, Some("false"), Some("false"), Some(subscribeCart.ftp_share_path), ftp_username, ftp_password, None, None, None, None, None, selectedColumns, Some(filterCond))

          println("SERVICE DETAILS ====> " + serviceDetails)
          val user = UserInfo(subscribeCart.user_id.toString, List(serviceDetails))

          println()
          println()
          println("USER OBJECT CREATED ==> " + user)
          //    sql"""INSERT INTO data_services(user_id, core_category_id, core_subcategory_id, metadata_core_id, purchased_date, status, service_type, start_date, row_delimiter, column_delimiter, is_encrypted, is_header, download_url, created_date, file_size_in_bytes, data_service_name, share_type, is_ftp_share, is_database_share, is_download_url, ftp_share_path, ftp_username, ftp_password, ftp_data_path, database_type, database_server_ip, database_instance_name, database_username, database_password, filter_condt, expired_date) VALUES (${subscribeCart.id}, ${subscribeCart.core_category_id}, ${subscribeCart.core_subcategory_id}, ${subscribeCart.metadata_core_id}, NOW(6), 1, ${subscribeCart.serviceType}, NOW(6), ${subscribeCart.row_delimeter}, ${subscribeCart.column_delimeter}, ${subscribeCart.is_encrypted}, ${subscribeCart.is_header}, "", NOW(6), NULL, ${approveInfo.data_service_name}, ${subscribeCart.shareType}, false, false, ${isDownloadUrl}, ${subscribeCart.ftp_share_path}, ${ftp_username}, ${ftp_password}, NULL, NULL, NULL, NULL, NULL, NULL, ${filterCond}, NULL)""".update.apply()

          try {
            CommonProcesses.ritualBeforeInsert(user)
          } catch {
            case ex: Exception => {
              println("---------------GOT ERROR -------------")
              ex.printStackTrace()
            }
          }
        }
        case 2 => {}
      }

    })

  }

  def applyForSubscription(mapIds: List[MapIds]) = {
    mapIds.foreach { x =>
      {
        sql"""UPDATE subscribeCart SET ApproveStatus = 0 WHERE id = ${x.id}""".update.apply()
      }
    }
  }

  def postAddToCart(userInfo: UserInfoForCart, selectedColumns: String) = {
    val user_id = userInfo.user_id
    val serviceDetails = userInfo.service_details

    serviceDetails.foreach(x => {
      val coreCategoryId = x.core_category_id
      val categoryName = (DB readOnly { implicit session => sql"""SELECT name FROM core_category WHERE id = ${coreCategoryId}""".map(rs => rs.string("name")).single.apply() }).get
      val coreSubCategoryId = x.core_subcategory_id
      val datasetName = (DB readOnly { implicit session => sql"""SELECT name FROM core_subcategory WHERE id = ${coreSubCategoryId}""".map(rs => rs.string("name")).single.apply() }).get
      val shareType = x.share_type
      val serviceType = x.service_type
      val columnDelimiter = x.column_delimiter
      val rowDelimiter = x.row_delimiter
      val filter = x.filter
      val ftpSharePath = x.ftp_share_path
      val markDelete = 0
      val createdBy = (DB readOnly { implicit session => sql"""SELECT user_id FROM users WHERE id = ${user_id}""".map(rs => rs.string("user_id")).single.apply() }).get
      val isSubscribe = 0
      val approveStatus = -1
      val isEncrypted = x.is_encrypted
      val isHeader = x.is_header
      val metaDataCoreId = x.metadata_core_id
      val ftp_username = x.ftp_username
      val ftp_password = x.ftp_password
      val summary = x.summary
      val desc = x.description
      val impCols = x.importantColumns
      val unitPrice = x.unitPrice
      val unitCurrency = x.unitCurrency

      sql"""INSERT INTO subscribeCart(user_id, core_category_id, category_name, core_subcategory_id, datasetName, shareType, serviceType, column_delimeter, row_delimeter, filter, selected_columns, ftp_share_path, MarkDelete, CreatedBy, CreationDate, SubscribeOn, IsSubscribe, ApproveStatus, ApprovedOrRejectBy, ApproveOrRejectDate, ApproverOrRejectorID, is_encrypted, is_header, metadata_core_id, ftp_username, ftp_password, Summary, Description, Important_Columns, Unit_Price, Unit_Currency) VALUES (${user_id}, ${coreCategoryId}, ${categoryName}, ${coreSubCategoryId}, ${datasetName}, ${shareType}, ${serviceType}, ${columnDelimiter}, ${rowDelimiter}, ${filter}, ${selectedColumns}, ${ftpSharePath}, ${markDelete}, ${createdBy}, NOW(6), NULL, ${isSubscribe}, ${approveStatus}, NULL, NULL, NULL, ${isEncrypted}, ${isHeader}, ${metaDataCoreId}, ${ftp_username}, ${ftp_password}, ${summary}, ${desc}, ${impCols}, ${unitPrice}, ${unitCurrency})""".update.apply()
    })
  }

  def postUnpublishDataset(unpublishDatasets: CoreSubCatIdAndUserInfo) = {
    val coreSubCategoryId = unpublishDatasets.coreSubCategoryId
    val modifiedBy = unpublishDatasets.createdBy.get
    val userId = unpublishDatasets.userId

    //    sql"""UPDATE published_subjects SET ModifiedBy=${modifiedBy}, ModificationDate=NOW(6), Status='unpublished' WHERE core_subcategory_id=${coreSubCategoryId}""".update.apply

    sql"""UPDATE published_subjects SET Status='unpublished', ModifiedBy=${modifiedBy}, ModificationDate=NOW(6) WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".update.apply()

    sql"""UPDATE publishedDatasetGroupMapping SET Status='unpublished', ModifiedBy=${modifiedBy}, ModificationDate=NOW(6) WHERE publishID = (SELECT id FROM published_subjects WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId})""".update.apply()

  }

  def postChangeVisibility(visibilityChange: PublishDatasets) = {
    val coreSubCategoryId = visibilityChange.coreSubCategoryId
    val userId = visibilityChange.userId
    val createdBy = visibilityChange.createdBy
    val groupIds = visibilityChange.groupIds
    val summary = visibilityChange.summary
    val description = visibilityChange.description
    val importantColumns = visibilityChange.importantColumns
    val unitPrice = visibilityChange.unitPrice
    val unitCurrency = visibilityChange.unitCurrency

    val publishedId = (DB readOnly { implicit session =>
      sql"""SELECT id FROM published_subjects WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".map(rs => rs.int("id")).single.apply()
    }).get

    sql"""UPDATE published_subjects SET Summary=$summary, Description=$description, Important_Columns=$importantColumns, Unit_Price=$unitPrice, Unit_Currency=$unitCurrency  WHERE id=$publishedId""".update.apply()
    sql"""UPDATE publishedDatasetGroupMapping SET MarkDelete=1, Status='unpublished', ModifiedBy=$createdBy, ModificationDate=NOW(6) WHERE publishID=$publishedId""".update.apply()

    groupIds.foreach { x =>
      {
        sql"""INSERT INTO publishedDatasetGroupMapping(publishID, groupID, CreatedBy, CreationDate, ModifiedBy, ModificationDate, MarkDelete, Status) VALUES ($publishedId, ${x.id}, $createdBy, NOW(6), NULL, NULL, 0, 'published')""".update.apply()
      }
    }
  }

  def postPublishDatasets(publishDatasets: PublishDatasets) = {

    val coreSubCategoryId = publishDatasets.coreSubCategoryId
    val userId = publishDatasets.userId
    val createdBy = publishDatasets.createdBy
    val groupIds = publishDatasets.groupIds
    val curDate = new Date().getTime
    val summary = publishDatasets.summary
    val desc = publishDatasets.description
    val impCols = publishDatasets.importantColumns
    val unitPrice = publishDatasets.unitPrice
    val unitCurrency = publishDatasets.unitCurrency
    val coreCategoryId = DB readOnly { implicit session =>
      sql"""SELECT core_category_id FROM core_subcategory WHERE id = ${coreSubCategoryId}""".map(rs => rs.int("core_category_id")).single.apply()
    }
    val publishedId = DB readOnly { implicit session =>
      sql"""SELECT id FROM published_subjects WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".map(rs => rs.int("id")).single.apply()
    }

    publishedId match {
      case None => {
        sql"""INSERT INTO published_subjects(core_category_id, core_subcategory_id, type, created_date, MarkDelete, CreatedBy, Status, Visibility, user_id, Summary, Description, Important_Columns, Unit_Price, Unit_Currency) VALUES (${coreCategoryId}, ${coreSubCategoryId}, 'D', $curDate, 0, ${createdBy}, 'published', 1, $userId, $summary, $desc, $impCols, $unitPrice, $unitCurrency)""".update.apply()

        val lastInsertedId = DB readOnly { implicit session =>
          sql"""SELECT MAX(id) id FROM published_subjects WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".map(rs => rs.int("id")).single.apply()
        }

        groupIds.foreach { x =>
          {
            sql"""INSERT INTO publishedDatasetGroupMapping(publishID, groupID, CreatedBy, CreationDate, ModifiedBy, ModificationDate, MarkDelete, Status) VALUES ($lastInsertedId, ${x.id}, $createdBy, NOW(6), NULL, NULL, 0, 'published')""".update.apply()
          }
        }
      }
      case Some(x) => {
        sql"""UPDATE published_subjects SET Status='published', ModifiedBy=$createdBy, ModificationDate=NOW(6) WHERE user_id = ${userId} AND core_subcategory_id = ${coreSubCategoryId}""".update.apply()

        sql"""UPDATE publishedDatasetGroupMapping SET MarkDelete=1, ModifiedBy=$createdBy, ModificationDate=NOW(6) WHERE publishID=${x}""".update.apply()

        groupIds.foreach { y =>
          {
            sql"""INSERT INTO publishedDatasetGroupMapping(publishID, groupID, CreatedBy, CreationDate, ModifiedBy, ModificationDate, MarkDelete, Status) VALUES ($x, ${y.id}, $createdBy, NOW(6), NULL, NULL, 0, 'published')""".update.apply()
          }
        }

      }
      case _ => {}
    }

  }

  def postSubjectArea(publishSA: PublishSubjectArea) = {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/

    val subjectAreaList = publishSA.colDetails.toList
    val curDate = new Date().getTime
    sql"""TRUNCATE table published_subjects""".execute.apply()

    subjectAreaList.foreach { z =>
      {
        val custList = z.columnName
        custList.foreach { x =>
          {
            x.Type match {
              case "D" => {
                val idInfo = DB readOnly { implicit session =>
                  sql"""SELECT id FROM core_category WHERE name = ${x.Subject_area}""".map(rs => rs.int("id")).single.apply()
                }
                sql"""INSERT INTO published_subjects(core_category_id, core_subcategory_id, type, created_date) VALUES ($idInfo, ${x.id}, 'D', $curDate)""".update.apply()
              }
              case "S" => {
                val idInfo = DB readOnly { implicit session =>
                  sql"SELECT  id FROM core_category WHERE name = ${x.Subject_area}".map(rs => rs.int("id")).single.apply()
                }
                sql"""INSERT INTO published_subjects(core_category_id, type, created_date) VALUES ($idInfo, 'S', $curDate)""".update.apply()
              }
              case _ => {

              }
            }
          }
        }
      }
    }

  }

  def postInsertScore(insertScore: InsertScore) = {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val (user_id, rating, subcategory_id,
      commen_header, comments, created_date) = (insertScore.user_id, insertScore.rating,
      insertScore.subcategory_id, insertScore.commen_header, insertScore.comments,
      new Date().getTime) //sdf.format(new Date()))

    try {

      sql"""insert into user_rating (user_id, rating, subcategory_id, commen_header, comments, created_date) values ($user_id, $rating, $subcategory_id, $commen_header, $comments, $created_date)""".update.apply()

    } catch {
      case ex: SQLException => {
        ex.printStackTrace
        throw new Exception("Could not perform update due to SQLException...Check the stacktrace above")
      }
      case ex: Exception => {
        ex.printStackTrace
        throw new Exception("Could not perform update due to Exception...Check the stacktrace above")
      }
    } finally {
      println("------------>UPDATION COMPLETE<--------------")
    }
  }

  def postNewRequest(newReq: NewRequest) {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/
    val sdf = new SimpleDateFormat("yyyyMMdd")

    val (fromUserId, toUserId, message, status, readStatus, curDate) = (newReq.fromUserId, newReq.toUserId, newReq.message, "Pending", "unread", new Date().getTime) //sdf.format(new Date()))

    sql"""INSERT INTO user_queries (from_user_id, to_user_id, ref_query_id, message, status, read_status, created_date)
VALUES ($fromUserId , $toUserId, NULL, $message, $status, $readStatus , $curDate);""".update.apply()
  }

  def postReplyRequest(replyReq: ReplyRequest) {
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession
    val sdf = new SimpleDateFormat("yyyyMMdd")

    val (fromUserId, toUserId, refQueryId, message, status, readStatus, curDate) = (replyReq.fromUserId, replyReq.toUserId, replyReq.refQueryId, replyReq.message, replyReq.status, "unread", new Date().getTime) //sdf.format(new Date()))

    sql"""INSERT INTO user_queries (from_user_id, to_user_id, ref_query_id, message, status, read_status, created_date) VALUES 
($fromUserId , $toUserId,  $refQueryId, $message, $status, $readStatus , $curDate);""".update.apply()
  }

  def postSendToAll(sendInfo: SendMessageToAll) {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/
    val sdf = new SimpleDateFormat("yyyyMMdd")
    case class Member(rs: WrappedResultSet)

    val memberId: List[String] = DB readOnly { implicit session =>
      sql"select id from users".map(rs => rs.string("id")).list.apply()
    }

    val (fromUserId, message, curDate) = (sendInfo.fromUserId, sendInfo.message, new Date().getTime) //sdf.format(new Date()))

    memberId.foreach(x => {
      val id = x
      sql"""INSERT INTO user_queries (from_user_id, to_user_id, ref_query_id, message, status, read_status, created_date) VALUES 
($fromUserId , $id,  null, $message, 'Pending', 'unread' , $curDate);""".update.apply()
    })

  }

  def insertInfoEihAccessInfoByUid(uid: Int, roles: List[Int], eihController: EihController) = {
    roles.foreach(eihController.insertIntoEihAccessInfo(uid, _))
  }

  def postUpdate(upd: Update) = {
    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/

    if (upd.isInstanceOf[UpdateUser]) {
      val userToUpdate = upd.asInstanceOf[UpdateUser]
      val (id, first_name, middle_name, last_name, user_id, password,
        mail_id, contact_no, created_date, is_admin, is_subscriber, is_publisher, is_searcher, is_ingester, sandboxLocation) = (userToUpdate.id, userToUpdate.first_name,
        userToUpdate.middle_name, userToUpdate.last_name, userToUpdate.user_id, userToUpdate.password, userToUpdate.mail_id,
        userToUpdate.contact_no, userToUpdate.created_date, userToUpdate.is_admin, userToUpdate.is_subscriber, userToUpdate.is_publisher, userToUpdate.is_searcher, userToUpdate.is_ingester, userToUpdate.sandboxlocation)

      var final_middle_name = ""

      middle_name match {
        case None => final_middle_name
        case Some(value) => final_middle_name = value
      }

      try {
        sql"""UPDATE users SET  first_name = $first_name, middle_name = $final_middle_name, last_name = $last_name, user_id = $user_id, password  = $password, mail_id = $mail_id, contact_no = $contact_no , created_date = $created_date, is_admin = $is_admin, is_subscriber = $is_subscriber, is_publisher  = $is_publisher, is_searcher = $is_searcher, is_ingester = $is_ingester  WHERE  id = $id""".update.apply()

        val isPathPresent = (DB readOnly { implicit session =>
          sql"""SELECT EXISTS(SELECT SandboxID FROM sandbox WHERE UserID = ${user_id}) SandboxID""".map(rs => rs.int("SandboxID")).single.apply()
        }).get

        isPathPresent match {
          case 0 => { sql"""INSERT INTO sandbox(SandboxID, UserID) VALUES (${sandboxLocation}, ${user_id})""".update.apply() }
          case 1 => { sql"""UPDATE sandbox SET SandboxID = ${sandboxLocation} WHERE UserID = ${user_id}""".update.apply() }
          case _ => {}
        }
      } catch {
        case ex: SQLException => {
          ex.printStackTrace
          throw new Exception("Could not perform update due to SQLException...Check the stacktrace above")
        }
        case ex: Exception => {
          ex.printStackTrace
          throw new Exception("Could not perform update due to Exception...Check the stacktrace above")
        }
      } finally {
        println("------------>UPDATION COMPLETE<--------------")
      }
    } else if (upd.isInstanceOf[UpdateReadFlag]) {
      val UpdtReadFlag = upd.asInstanceOf[UpdateReadFlag]
      val queryId = UpdtReadFlag.queryId

      sql"""UPDATE user_queries SET read_status = 'read' WHERE query_id = $queryId""".update.apply()
    } else if (upd.isInstanceOf[DownloadUrlPath]) {
      val downloadUrlPath = upd.asInstanceOf[DownloadUrlPath]
      val urlPath = downloadUrlPath.urlPath

      val data_service_id = DB readOnly { implicit session =>
        sql"select max(data_service_id) data_service_id from data_services".map(rs => rs.string("data_service_id")).single.apply()
      }

      data_service_id match {
        case Some(x) => sql"""UPDATE data_services SET download_url = $urlPath where data_service_id=$x""".update.apply()
        case None => throw new Exception("no max data_service_id found in data_services")
      }
    }
  }

  def getNewlyCreatedUserId(newUser: UserEntry): Int = {
    val user_id = newUser.user_id
    val password = newUser.password
    val newlyCreatedId = (sql"""select max(id) id from users where user_id = ${user_id} and password = ${password}""".map(rs => rs.int("id")).single.apply()).get
    newlyCreatedId
  }

  def postAddUser(newUser: UserEntry) = {

    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    implicit val session = AutoSession*/

    val (first_name, middle_name, last_name, user_id, password,
      mail_id, contact_no, is_admin, is_subscriber, is_publisher, is_searcher, is_ingester, sandboxLocation) = (
      newUser.first_name,
      newUser.middle_name, newUser.last_name, newUser.user_id, newUser.password, newUser.mail_id,
      newUser.contact_no, newUser.is_admin, newUser.is_subscriber, newUser.is_publisher, newUser.is_searcher, newUser.is_ingester, newUser.sandboxlocation)
    var final_middle_name = ""

    middle_name match {
      case None => final_middle_name
      case Some(value) => final_middle_name = value
    }

    sql"""insert into users( first_name, middle_name, last_name, user_id, password, mail_id, contact_no, is_admin, created_date, is_subscriber, is_publisher, is_searcher, is_ingester) values ($first_name, $final_middle_name, $last_name, $user_id, $password, $mail_id, $contact_no, $is_admin, Current_Date, $is_subscriber, $is_publisher, $is_searcher, $is_ingester)""".update.apply()
    sql"""insert into sandbox(SandboxID,UserID) values ($sandboxLocation, $user_id)""".update.apply()

  }

  def postRunUpdate(updateValues: DataServiceUpdate) = {

    /*Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)
    implicit val session = AutoSession*/

    val status = updateValues.status
    val serviceType = updateValues.service_type
    val dataServiceId = updateValues.data_service_id

    sql"""UPDATE data_services SET status=$status,service_type=$serviceType where data_service_id=$dataServiceId""".update.apply()

  }
  def postRunInsert(insertValues: DataServicesValues) = {

    val user_id = insertValues.user_id
    val core_category_id = insertValues.core_category_id
    val core_subcategory_id = insertValues.core_subcategory_id
    val metadata_core_id = insertValues.metadata_core_id
    val stat = insertValues.status
    val service_type = insertValues.service_type
    val start_date = insertValues.start_date
    val row_delimiter = insertValues.row_delimiter
    val column_delimiter = insertValues.column_delimiter
    val is_encrypted = insertValues.is_encrypted
    val is_header = insertValues.is_header
    val download_url = insertValues.download_url
    val data_services_element = insertValues.data_services_element
    val operation_flg = insertValues.operation_flg
    val data_service_id = insertValues.data_service_id
    val data_service_name = insertValues.data_service_name
    val share_type = insertValues.share_type
    val is_ftp_share = insertValues.is_ftp_share
    val is_database_share = insertValues.is_database_share
    val ftp_share_path = insertValues.ftp_share_path
    val ftp_username = insertValues.ftp_username
    val ftp_password = insertValues.ftp_password
    val database_type = insertValues.database_type
    val database_server_ip = insertValues.database_server_ip
    val database_instance_name = insertValues.database_instance_name
    val database_username = insertValues.database_username
    val database_password = insertValues.database_password
    val filter = insertValues.filter
    val o_exe_status = "@o_exe_status"
    var final_status: Int = 0
    var final_is_encrypted = false
    var final_is_header = false
    var final_is_ftp_share = false
    var final_is_database_share = false
    var final_ftp_share_path = ""
    var final_ftp_username = ""
    var final_ftp_password = ""
    var final_database_type = ""
    var final_database_server_ip = ""
    var final_database_instance_name = ""
    var final_database_username = ""
    var final_database_password = ""
    var final_filter = ""
    var is_download_url = false

    stat match {
      case None => final_status = 1
      case Some(1) => final_status = 1
      case Some(0) => final_status = 0
      case _ => {}
    }

    is_encrypted match {
      case None => final_is_encrypted = true
      case Some(true) => final_is_encrypted = true
      case Some(false) => final_is_encrypted = false
      case _ => {}
    }

    is_header match {
      case None => final_is_header = true
      case Some(true) => final_is_header = true
      case Some(false) => final_is_header = false
      case _ => {}
    }

    is_ftp_share match {
      case None => final_is_ftp_share
      case Some(value) => final_is_ftp_share = value
      case _ => {}
    }

    is_database_share match {
      case None => final_is_database_share
      case Some(value) => final_is_database_share = value
      case _ => {}
    }

    ftp_share_path match {
      case None => final_ftp_share_path
      case Some(value) => final_ftp_share_path = value
      case _ => {}
    }

    ftp_username match {
      case None => final_ftp_username
      case Some(value) => final_ftp_username = value
      case _ => {}
    }

    ftp_password match {
      case None => final_ftp_password
      case Some(value) => final_ftp_password = value
      case _ => {}
    }

    database_type match {
      case None => final_database_type
      case Some(value) => final_database_type = value
      case _ => {}
    }

    database_server_ip match {
      case None => final_database_server_ip
      case Some(value) => final_database_server_ip = value
      case _ => {}
    }

    database_instance_name match {
      case None => final_database_instance_name
      case Some(value) => final_database_instance_name = value
      case _ => {}
    }

    database_username match {
      case None => final_database_username
      case Some(value) => final_database_username = value
      case _ => {}
    }

    database_password match {
      case None => final_database_password
      case Some(value) => final_database_password = value
      case _ => {}
    }

    share_type match {
      case "url" => is_download_url = true
      case _ => is_download_url = false
    }

    filter match {
      case None => final_filter
      case Some(value) => final_filter = value
      case _ => {}
    }

    Class.forName("com.mysql.jdbc.Driver")
    connection = DriverManager.getConnection(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    connection = ds.getConnection*/
    //val statement = connection.createStatement

    val callableStatement = connection.prepareCall("CALL daas_sp_insert_update_user_services(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")

    callableStatement.setLong(1, user_id)
    callableStatement.setLong(2, core_category_id)
    callableStatement.setLong(3, core_subcategory_id)
    callableStatement.setLong(4, metadata_core_id)
    callableStatement.setInt(5, final_status)
    callableStatement.setString(6, service_type)
    callableStatement.setString(7, start_date)
    callableStatement.setString(8, row_delimiter)
    callableStatement.setString(9, column_delimiter)
    callableStatement.setBoolean(10, final_is_encrypted)
    callableStatement.setBoolean(11, final_is_header)
    callableStatement.setString(12, download_url)
    callableStatement.setString(13, data_services_element)
    callableStatement.setString(14, operation_flg)
    callableStatement.setInt(15, data_service_id)
    callableStatement.setString(16, o_exe_status)
    callableStatement.setString(17, data_service_name)
    callableStatement.setString(18, share_type)
    callableStatement.setBoolean(19, final_is_ftp_share)
    callableStatement.setBoolean(20, final_is_database_share)
    callableStatement.setString(21, final_ftp_share_path)
    callableStatement.setString(22, final_ftp_username)
    callableStatement.setString(23, final_ftp_password)
    callableStatement.setString(24, final_database_type)
    callableStatement.setString(25, final_database_server_ip)
    callableStatement.setString(26, final_database_instance_name)
    callableStatement.setString(27, final_database_username)
    callableStatement.setString(28, final_database_password)
    callableStatement.setBoolean(29, is_download_url)
    callableStatement.setString(30, final_filter)

    callableStatement.executeUpdate()
    connection.close()

  }

  def runSubjectAreaReport(sql: String, parameters: List[String], f: (WrappedResultSet => JValue)): List[JValue] = {
    /*
    // For Tomcat
    val ds = (new InitialContext)
      .lookup("java:/comp/env").asInstanceOf[Context]
      .lookup("jdbc/DSTest").asInstanceOf[DataSource]


    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))

*/

    // For Jetty
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data: List[JValue] = DB readOnly { implicit session => SQL(sql).bind(parameters: _*).map(f).list.apply() }
    //val data : List[JValue] = DB readOnly { implicit session => SQL(sql).map(f).list.apply() }

    //render( data.foldLeft(JNothing: JValue)(_ merge _) )
    //render(data)
    data

  }

  def runReport(sql: String, parameters: List[String], f: (WrappedResultSet => JValue)): JValue = {
    /*
    // For Tomcat
    val ds = (new InitialContext)
      .lookup("java:/comp/env").asInstanceOf[Context]
      .lookup("jdbc/DSTest").asInstanceOf[DataSource]


    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))

*/

    // For Jetty
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data: List[JValue] = DB readOnly { implicit session => SQL(sql).bind(parameters: _*).map(f).list.apply() }
    //val data : List[JValue] = DB readOnly { implicit session => SQL(sql).map(f).list.apply() }

    //render( data.foldLeft(JNothing: JValue)(_ merge _) )
    render(data)

  }

  def runGeneralQuery(sql: String, f: (WrappedResultSet => JValue)): List[JValue] = {

    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data: List[JValue] = DB readOnly { implicit session => SQL(sql).map(f).list.apply() }
    //val data : JValue = DB readOnly { implicit session => SQL(sql).bind(parameters:_*).map(f) }
    //    render(data)
    data
  }

  def runQuickViewSingleParamReport(sql: String, parameter: String, f: (WrappedResultSet => JValue)): JValue = {
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data: List[JValue] = DB readOnly { implicit session => SQL(sql).bind(parameter).map(f).list.apply() }
    //val data : JValue = DB readOnly { implicit session => SQL(sql).bind(parameters:_*).map(f) }
    render(data)
  }

  def runQuickViewReport(sql: String, parameters: List[String], f: (WrappedResultSet => JValue)): JValue = {

    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /*val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data: List[JValue] = DB readOnly { implicit session => SQL(sql).bind(parameters: _*).map(f).list.apply() }
    //val data : JValue = DB readOnly { implicit session => SQL(sql).bind(parameters:_*).map(f) }
    render(data)
  }

  def runParamLessReport(sql: String, f: (WrappedResultSet => JValue)): JValue = {

    // For Jetty
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.singleton(driver, db_user_name, db_password)

    /* val ds = DatabaseConn.getSqlDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val data2: List[JValue] = DB readOnly { implicit session => SQL(sql).map(f).list.apply() }

    //render( data.foldLeft(JNothing: JValue)(_ merge _) )
    render(data2)

  }

  def runParamLessHiveReport(sql: String, f: (WrappedResultSet => JValue), db: String): Option[JValue] = {
    import java.sql._

    //commented TODO
    /*val fin_map = (rs: ResultSet) => {

      val str = new StringBuilder
      str ++= "{ "
      for (i <- 1 until rs.getMetaData.getColumnCount) {
        //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
        str ++= "\"" + rs.getMetaData.getColumnName(i) + "\" : \"" + rs.getString(rs.getMetaData.getColumnName(i)) + "\","
      }
      str ++= "\"" + rs.getMetaData.getColumnName(rs.getMetaData.getColumnCount) + "\" : \"" + rs.getString(rs.getMetaData.getColumnName(rs.getMetaData.getColumnCount)) + "\" }"

      parse(str.toString.replaceAll("\n", " "))
    }*/

    // For Jetty
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    //ConnectionPool.singleton(hive_driver, hive_user_name, hive_password)

    /*    val ds = DatabaseConn.getHiveDataSource()
    ConnectionPool.singleton(new DataSourceConnectionPool(ds))*/

    val con = DriverManager.getConnection(hive_driver, hive_user_name, hive_password)

    //commented TODO
    statement = con.createStatement()
    rs = statement.executeQuery(sql)
    var data1: Option[List[JValue]] = None

    //commented TODO
    while (rs.next()) {
      //                System.out.println(rs.getString("node_id"));--------(rs: ResultSet) =>
      val fin_map = {

        val str = new StringBuilder
        str ++= "{ "
        for (i <- 1 until rs.getMetaData.getColumnCount) {
          //( "id" -> rs.string( "id" ) )~( "name" -> rs.string( "name" ) )
          str ++= "\"" + rs.getMetaData.getColumnName(i) + "\" : \"" + rs.getString(rs.getMetaData.getColumnName(i)) + "\","
        }
        str ++= "\"" + rs.getMetaData.getColumnName(rs.getMetaData.getColumnCount) + "\" : \"" + rs.getString(rs.getMetaData.getColumnName(rs.getMetaData.getColumnCount)) + "\" }"

        parse(str.toString.replaceAll("\n", " "))
      }

      data1 match {

        case None => { data1 = Some(List(fin_map)) }
        case _ => { data1 = Some(data1.get :+ fin_map) }
      }

      //     data1 = Some(fin_map)
    }

    statement.close
    con.close

    //    val data2: List[JValue] = DB readOnly { implicit session => SQL(sql).map(f).list.apply() }

    //    render( data.foldLeft(JNothing: JValue)(_ merge _) )
    //        println("DATA1$$$$$$$$: "+data1.get)
    //    println("DATA2$$$$$$$$: " + data2)
    var result: Option[JValue] = None

    data1 match {
      case Some(v) => result = Some(render(v))
      case None => result = None
    }
    result

  }

  def hadoopPut(fileName: String, subjectArea: String, datasetName: String, hadoopFile: String, sandBoxID: String): String = {
    val fileLocation = conf.getString("downloadDataPath.path") + fileName
    val fileDir = fileName.slice(0, fileName.lastIndexOf("."))
    val fileext = fileName.slice(fileName.lastIndexOf("."), fileName.length)
    val hadoopFileName = hadoopFile + fileext
    val sandBoxId = sandBoxID

    println("fileDir ==>" + fileDir)
    println("Old File Name ===>" + fileName)
    println("New File Name ===>" + hadoopFileName)

    s"sudo -u hdfs hadoop fs -mkdir -p ${sandBoxId}${fileDir}".!
    s"sudo -u hdfs hadoop fs -chmod -R 777 /daas".!
    s"sudo -u hdfs hadoop fs -copyFromLocal -f ${fileLocation} ${sandBoxId}${fileDir}".!
    s"sudo -u hdfs hadoop fs -mv ${sandBoxId}${fileDir}/${fileName} ${sandBoxId}${fileDir}/${hadoopFileName}".!

    val hadoopFileSavePath = s"${sandBoxId}${fileDir}/${hadoopFileName}"
    hadoopFileSavePath
  }

  def sandboxService(user: String, fileName: String, delimiter: String, subjectArea: String, datasetName: String, sandBoxID: String): String = {
    val fileDir = fileName.slice(0, fileName.lastIndexOf("."))
    val tableName = subjectArea + "_" + datasetName + "_" + System.currentTimeMillis() / 1000
    val returnPath = hadoopPut(fileName, subjectArea, datasetName, tableName, sandBoxID)
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val con = DriverManager.getConnection(hive_base_driver, hive_user_name, hive_password)

    val fileLocation = conf.getString("downloadDataPath.path") + fileName

    statement = con.createStatement()
    var sql = s"CREATE DATABASE IF NOT EXISTS ${user}"

    statement.executeUpdate(sql)
    val fileHeadings = scala.io.Source.fromFile(fileLocation).getLines().take(1).mkString

    println()
    println()
    println("**********FILEHEADINGS************" + fileHeadings)
    println()
    var splitChar = ' '

    delimiter match {
      case "PIPE" => splitChar = '|'
      case "TAB" => splitChar = ' '
    }
    var cols = fileHeadings.split(splitChar).toList

    cols = cols.foldLeft(List[String]())((c, r) => { c :+ (r.slice(r.indexOf(".") + 1, r.length)) })

    sql = s"CREATE EXTERNAL TABLE IF NOT EXISTS ${user}.${tableName}("

    sql = cols.foldLeft(sql)((c, r) => { c + r + " STRING," }).dropRight(1)

    sql += s""")
ROW FORMAT 
DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '${sandBoxID}${fileDir}'"""

    println("SQL COMMAND: " + sql)

    statement.executeUpdate(sql)

    returnPath
  }

}
