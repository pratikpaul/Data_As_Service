package com.self.dataAsService

import scalikejdbc.WrappedResultSet
import org.apache.commons.lang._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object RegisterReport {

  def registerAllReports {
    registerGetGDPR
    registerGetUnmappedDataset
    registerGetAppConfigData
    registerGetAllUnapprovedCartData
    registerGetFromCartByIdForSubscriber
    registerGetFromCartByIdForSubscriber2
    registerAllMappedSubjectAreaForPublisher
    registerGetPublishedDatasetGroupMapping
    registerSubCategoryGroupList
    registerUserGroupList
    registerGroupUserMapDetails
    registerGroupSubjectAreaMapDetails
    registerGroupAndSubGroup
    registerFTPDetails
    registerGetAdminId
    registerGetAllSubjectArea
    registerGetFeedbacksInbox
    registerGetFeedbacksSentItem
    registerGetUser
    registerGetScore
    registerGetCount
    registerCoreCategory
    registerCoreSubCategory
    registerCoreFilteredData
    registerFilteredDataServices
    registerDataInsertion
    registerNotificationLog
    registerAdminPage
    registerAdminColumnCheck
    registerAdminPageAlt
    registerLoginPage
    registerScoreCard
    typeInfoType_D
    typeInfoType_S
    registerQuickView
    registerDownloadData
    registerCoreCateg
    registerPieChart
    registerBarChart
    registerGatherDownloadHistory
  }

  //changed here
  def registerDataInsertion {
    Drive.registerForPost("insertData")
  }
  //change done

  def registerGetAdminId {
    var sql = """SELECT id
    |from users
    |where user_id = 'admin'""".stripMargin

    val map = (rs: WrappedResultSet) => {
      parse(s"""{"id" : "${rs.string("id")}"} """)
    }

    Drive.register("getAdminId", sql, map)
  }

  def registerGroupAndSubGroup {
    var sql = """SELECT Id groupId,
          | GroupName groupName,
          | ParentId parentId,
          | MarkDelete markDelete,
          | CreatedBy createdBy,
          | CreationDate creationDate,
          | ModifiedBy modifiedBy,
          | ModificationDate modificationDate
          | from tGroup
          | WHERE 1 = 1 and markDelete = 0""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("groupId" -> rs.string("groupId")) ~ ("groupName" -> rs.string("groupName")) ~ ("parentId" -> rs.string("parentId")) ~
        ("markDelete" -> rs.string("markDelete")) ~ ("createdBy" -> rs.string("createdBy")) ~
        ("creationDate" -> rs.string("creationDate")) ~ ("modifiedBy" -> rs.string("modifiedBy")) ~ ("modificationDate" -> rs.string("modificationDate"))
    }

    Drive.register("getGroupDetails", sql, map)
  }

  def registerGetGDPR {
    var sql = """SELECT a.id AS MapId,
          | b.id AS GDPRID,
          | a.DataType datatype,
          | b.GDPRRuleName GDPRRuleName,
          | b.DisplayName displayName,
          | b.Comment comment
          | FROM GDPRandDataTypeMap a,
          | GDPRRules b 
          | WHERE 1=1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("MapId" -> rs.int("MapId")) ~ ("GDPRID" -> rs.int("GDPRID")) ~ ("datatype" -> rs.string("datatype")) ~
        ("GDPRRuleName" -> rs.string("GDPRRuleName")) ~ ("displayName" -> rs.string("displayName")) ~
        ("comment" -> rs.string("comment"))
    }
    
    Drive.register("getGDPRRulesDetails", sql, map)
  }

  def registerGetUnmappedDataset {
    var sql = """SELECT cs.id core_subcategory_id, 
          | cs.name core_subcategory_name
          | FROM core_subcategory cs 
          | LEFT JOIN groupSubjectAreaMap gsa ON cs.id = gsa.SubCategoryId 
          | WHERE gsa.groupid IS NULL""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("core_subcategory_id" -> rs.int("core_subcategory_id")) ~ ("core_subcategory_name" -> rs.string("core_subcategory_name"))
    }

    Drive.register("getUnmappedDataset", sql, map)
  }

  def registerFTPDetails {
    var sql = """SELECT ID id,
        | FTP_Name ftp_name,
        | FTP_Server ftp_server,
        | FTP_User_Name ftp_user_name,
        | FTP_Password ftp_password,
        | FTP_Port ftp_port,
        | FTP_Default_location ftp_default_location
        | FROM ftp_master""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("ftp_name" -> rs.string("ftp_name")) ~
        ("ftp_server" -> rs.string("ftp_server")) ~ ("ftp_user_name" -> rs.string("ftp_user_name")) ~
        ("ftp_password" -> rs.string("ftp_password")) ~ ("ftp_port" -> rs.string("ftp_port")) ~
        ("ftp_default_location" -> rs.string("ftp_default_location"))
    }

    Drive.register("getFTPDetails", sql, map)
  }

  def registerGetPublishedDatasetGroupMapping {
    var sql = """SELECT id,
          | publishID,
          | groupID,
          | CreatedBy createdBy,
          | CreationDate creationDate,
          | ModifiedBy modifiedBy,
          | ModificationDate modificationDate,
          | MarkDelete markDelete,
          | Status status
          | FROM publishedDatasetGroupMapping
          | WHERE 1=1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.string("id")) ~ ("publishID" -> rs.string("publishID")) ~ ("groupID" -> rs.string("groupID")) ~
        ("createdBy" -> rs.string("createdBy")) ~ ("modifiedBy" -> rs.string("modifiedBy")) ~ ("modificationDate" -> rs.string("modificationDate")) ~
        ("markDelete" -> rs.string("markDelete")) ~ ("status" -> rs.string("status"))
    }

    Drive.register("getPublishedDatasetGroupMapping", sql, map)
  }

  def registerAllMappedSubjectAreaForPublisher {
    var sql = """SELECT DISTINCT id,
          | name,
          | Type,
          | Subject_area_id,
          | Subject_area ,
          | checked,
          | CreatedBy,
          | CreationDate,
          | ModifiedBy,
          | ModificationDate,
          | Visibility,
          | Status,
          | Summary summary,
          | Description description,
          | Important_Columns important_columns,
          | Unit_Price unit_price,
          | Unit_Currency unit_currency
          | FROM( SELECT a.id,
          | a.name,
          | 'D' TYPE,
          | C.CreatedBy,
          | C.CreationDate,
          | C.ModifiedBy,
          | C.ModificationDate,
          | C.Visibility,
          | C.Status,
          | C.Summary,
          | C.Description,
          | C.Important_Columns,
          | C.Unit_Price,
          | C.Unit_Currency,
          | a.id Subject_area_id,
          | b.name Subject_area,
          | (CASE WHEN C.core_subcategory_id IS NULL THEN 'false' ELSE 'true' END) checked
          | FROM core_subcategory a 
          | JOIN core_category b 
          | ON   a.core_category_id = b.id 
          | LEFT JOIN published_subjects C
          | ON C.core_subcategory_id = a.id""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("name" -> rs.string("name")) ~ ("Type" -> rs.string("Type")) ~
        ("CreatedBy" -> rs.string("CreatedBy")) ~ ("CreationDate" -> rs.string("CreationDate")) ~ ("checked" -> rs.string("checked").toBoolean) ~
        ("ModifiedBy" -> rs.string("ModifiedBy")) ~ ("ModificationDate" -> rs.string("ModificationDate")) ~
        ("Visibility" -> rs.string("Visibility")) ~ ("Status" -> rs.string("Status")) ~ ("Subject_area_id" -> rs.string("Subject_area_id")) ~
        ("Subject_area" -> rs.string("Subject_area")) ~
        ("summary" -> rs.string("summary")) ~ ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency"))
    }

    Drive.register("getMappedSubjectAreaForPublisher", sql, map)
  }

  def registerGetAllUnapprovedCartData {
    var sql = """SELECT id,
          | user_id,
          | core_category_id,
          | category_name,
          | core_subcategory_id,
          | datasetName,
          | shareType,
          | serviceType,
          | column_delimeter,
          | row_delimeter,
          | filter,
          | selected_columns,
          | ftp_share_path,
          | MarkDelete,
          | CreatedBy,
          | CreationDate,
          | SubscribeOn,
          | IsSubscribe,
          | ApproveStatus,
          | ApprovedOrRejectBy,
          | ApproveOrRejectDate,
          | ApproverOrRejectorID,
          | ApproveOrRejectComment,
          | is_encrypted,
          | is_header,
          | metadata_core_id,
          | Summary summary,
          | Description description,
          | Important_Columns important_columns,
          | Unit_Price unit_price,
          | Unit_Currency unit_currency,
          | Mark_Read mark_read
          | FROM subscribeCart AS A
          | WHERE 1=1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("user_id" -> rs.string("user_id")) ~ ("core_category_id" -> rs.string("core_category_id")) ~
        ("category_name" -> rs.string("category_name")) ~ ("core_subcategory_id" -> rs.string("core_subcategory_id")) ~ ("datasetName" -> rs.string("datasetName")) ~
        ("shareType" -> rs.string("shareType")) ~ ("serviceType" -> rs.string("serviceType")) ~ ("column_delimeter" -> rs.string("column_delimeter")) ~
        ("row_delimeter" -> rs.string("row_delimeter")) ~ ("filter" -> rs.string("filter")) ~ ("selected_columns" -> parse(rs.string("selected_columns"))) ~
        ("ftp_share_path" -> rs.string("ftp_share_path")) ~ ("MarkDelete" -> rs.string("MarkDelete")) ~ ("CreatedBy" -> rs.string("CreatedBy")) ~
        ("CreationDate" -> rs.string("CreationDate")) ~ ("SubscribeOn" -> rs.string("SubscribeOn")) ~ ("IsSubscribe" -> rs.string("IsSubscribe")) ~
        ("ApproveStatus" -> rs.string("ApproveStatus")) ~ ("ApprovedOrRejectBy" -> rs.string("ApprovedOrRejectBy")) ~
        ("ApproveOrRejectDate" -> rs.string("ApproveOrRejectDate")) ~ ("ApproverOrRejectorID" -> rs.string("ApproverOrRejectorID")) ~ ("ApproveOrRejectComment" -> rs.string("ApproveOrRejectComment")) ~ ("is_encrypted" -> rs.string("is_encrypted")) ~
        ("is_header" -> rs.string("is_header")) ~ ("metadata_core_id" -> rs.string("metadata_core_id")) ~
        ("summary" -> rs.string("summary")) ~ ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency")) ~ ("mark_read" -> rs.string("mark_read"))
    }

    Drive.register("getCartData", sql, map)
  }

  def registerGetAllSubjectArea {
    var sql = """SELECT DISTINCT id,
          | name,
          | Type,
          | Subject_area_id,
          | Subject_area ,
          | checked,
          | Status,
          | Summary summary,
          | Description description,
          | Important_Columns important_columns,
          | Unit_Price unit_price,
          | Unit_Currency unit_currency
          | FROM( SELECT a.id,
          | a.name,
          | 'D' TYPE,
          | C.Status,
          | C.Summary,
          | C.Description,
          | C.Important_Columns,
          | C.Unit_Price,
          | C.Unit_Currency,
          | a.id Subject_area_id,
          | b.name Subject_area,
          | (CASE WHEN C.core_subcategory_id IS NULL THEN 'false' ELSE 'true' END) checked
          | FROM core_subcategory a 
          | JOIN core_category b 
          | ON   a.core_category_id = b.id 
          | LEFT JOIN published_subjects C
          | ON C.core_subcategory_id = a.id
          | ) C""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("name" -> rs.string("name")) ~ ("Type" -> rs.string("Type")) ~ ("Subject_area_id" -> rs.string("Subject_area_id")) ~
        ("Subject_area" -> rs.string("Subject_area")) ~ ("checked" -> rs.string("checked").toBoolean) ~ ("Status" -> rs.string("Status")) ~
        ("summary" -> rs.string("summary")) ~ ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency"))
    }

    Drive.register("getAllSubjectArea", sql, map)
  }

  def registerGetFeedbacksInbox {
    var sql = """SELECT  query_id,
    |from_user_id,
    |b.first_name from_username,
    |to_user_id,
    |c.first_name to_username,
    |ref_query_id,
    |message,
    |status,
    |read_status,
    |if(read_status = 'unread','false','true') read_flag,
    |a.created_date
    |FROM user_queries a
    |JOIN users b
    |ON a.from_user_id = b.id
    |JOIN users c
    |ON a.to_user_id = c.id
    |WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("query_id" -> rs.string("query_id")) ~ ("from_user_id" -> rs.string("from_user_id")) ~ ("from_username" -> rs.string("from_username")) ~
        ("to_user_id" -> rs.string("to_user_id")) ~ ("to_username" -> rs.string("to_username")) ~ ("ref_query_id" -> rs.string("ref_query_id")) ~
        ("message" -> rs.string("message")) ~ ("status" -> rs.string("status")) ~ ("read_status" -> rs.string("read_status")) ~
        ("read_flag" -> rs.string("read_flag")) ~ ("created_date" -> rs.string("created_date"))
    }

    Drive.register("getFeedbacksInbox", sql, map)
  }

  def registerGetFeedbacksSentItem {
    var sql = """SELECT  query_id,
    |from_user_id,
    |b.first_name from_username,
    |to_user_id,
    |c.first_name to_username,
    |ref_query_id,
    |message,
    |status,
    |read_status,
    |if(read_status = 'unread','false','true') read_flag,
    |a.created_date
    |FROM user_queries a
    |JOIN users b
    |ON a.from_user_id = b.id
    |JOIN users c
    |ON a.to_user_id = c.id
    |WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("query_id" -> rs.string("query_id")) ~ ("from_user_id" -> rs.string("from_user_id")) ~ ("from_username" -> rs.string("from_username")) ~
        ("to_user_id" -> rs.string("to_user_id")) ~ ("to_username" -> rs.string("to_username")) ~ ("ref_query_id" -> rs.string("ref_query_id")) ~
        ("message" -> rs.string("message")) ~ ("status" -> rs.string("status")) ~ ("read_status" -> rs.string("read_status")) ~
        ("read_flag" -> rs.string("read_flag")) ~ ("created_date" -> rs.string("created_date"))
    }

    Drive.register("getFeedbacksSentItem", sql, map)
  }

  def registerGetCount {
    var sql = """SELECT subcategory_id, 
    | rating, 
    | COUNT(*) no_of_cnts, 
    | 5 max_cnts FROM user_rating 
    | WHERE 1= 1""".stripMargin

    /*val map  = (rs: WrappedResultSet) => {
      ("subcategory_id" -> ("star" -> rs.string("rating")) ~ ("count" -> rs.string("no_of_cnts")))
    }*/

    val map = (rs: WrappedResultSet) => {
      ("subcategory_id" -> rs.string("subcategory_id")) ~ ("name" -> rs.string("rating")) ~ ("value" -> rs.string("no_of_cnts"))
    }

    Drive.register("getCount", sql, map)
  }

  def registerGetUser {
    var sql = """SELECT u.id id, 
  	| u.first_name,
  	| u.middle_name,
  	| u.last_name,
  	| u.user_id,
  	| u.password,
  	| u.mail_id,
  	| u.contact_no ,
  	| u.created_date ,
  	| u.is_admin,
  	| u.is_subscriber,
  	| u.is_publisher,
  	| u.is_searcher,
  	| u.is_ingester,
  	| u.last_login,
  	| s.SandboxID sandBoxID,
  	| (SELECT GROUP_CONCAT(role) roles FROM ACCESS_INFO
  	| WHERE role_id IN (
  	| SELECT role_id FROM EIH_AUTHORIZATION.EIH_ACCESS_INFO WHERE uid=u.id
  	| )) roles
  	| FROM users u
  	| LEFT OUTER JOIN  
  	| sandbox s ON u.user_id=s.UserID
  	| WHERE 1= 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.string("id")) ~ ("first_name" -> rs.string("first_name")) ~
        ("middle_name" -> rs.string("middle_name")) ~ ("last_name" -> rs.string("last_name")) ~
        ("user_id" -> rs.string("user_id")) ~ ("password" -> rs.string("password")) ~
        ("mail_id" -> rs.string("mail_id")) ~ ("contact_no" -> rs.string("contact_no")) ~
        ("created_date" -> rs.string("created_date")) ~ ("is_admin" -> rs.string("is_admin")) ~
        ("is_subscriber" -> rs.string("is_subscriber")) ~ ("is_publisher" -> rs.string("is_publisher")) ~
        ("is_searcher" -> rs.string("is_searcher")) ~ ("is_ingester" -> rs.string("is_ingester")) ~
        ("last_login" -> rs.string("last_login")) ~ ("sandBoxID" -> rs.string("sandBoxID")) ~
        ("roles" -> rs.string("roles"))
    }

    Drive.register("getUser", sql, map)
  }

  def registerGetScore {
    var sql = """SELECT rating_id,
    | id, 
    | first_name,
    | middle_name,
    | last_name,
    | u.user_id user_id,
    | rating,
    | subcategory_id ,
    | commen_header,
    | comments,
    | ur.created_date created_date
    | FROM  user_rating ur
    | JOIN users u
    | ON  ur.user_id = u.id""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("rating_id" -> rs.string("rating_id")) ~ ("id" -> rs.string("id")) ~ ("first_name" -> rs.string("first_name")) ~
        ("middle_name" -> rs.string("middle_name")) ~ ("last_name" -> rs.string("last_name")) ~
        ("user_id" -> rs.string("user_id")) ~ ("rating" -> rs.string("rating")) ~
        ("subcategory_id" -> rs.string("subcategory_id")) ~ ("commen_header" -> rs.string("commen_header")) ~
        ("comments" -> rs.string("comments")) ~ ("created_date" -> rs.string("created_date"))
    }

    Drive.register("getScore", sql, map)
  }

  def registerCoreCateg {

    var sql = """SELECT id, name, Type
	  | FROM(
	  | SELECT id, name, 'D' Type
	  | FROM core_subcategory
	  | UNION ALL
	  | SELECT id, name, 'S' Type
	  | FROM core_category
	  | ) C""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("name" -> rs.string("name")) ~ ("Type" -> rs.string("Type"))
    }
    Drive.register("searchField", sql, map);

  }

  def typeInfoType_S {
    val sql = """SELECT a.id subcategory_id,
          | a.name subcategory_name,
          | a.core_category_id category_id,
          | b.name category_name,
          | c.Summary summary,
          | c.Description description,
          | c.Important_Columns important_columns,
          | c.Unit_Price unit_price,
          | c.Unit_Currency unit_currency,
          | sub_cat_id,
          | average_rating,
          | no_of_rating,
          | '5' max_rating
          | FROM core_subcategory a
          | JOIN core_category b ON b.id = a.core_category_id
          | JOIN published_subjects c on a.id = c.core_subcategory_id
          | LEFT JOIN (SELECT subcategory_id sub_cat_id, avg(rating) average_rating,
          | Count(*) no_of_rating FROM user_rating GROUP BY subcategory_id) m  ON a.id = m.sub_cat_id
          | WHERE b.id = """.stripMargin

    val map = (rs: WrappedResultSet) => {
      ("subcategory_id" -> rs.string("subcategory_id")) ~
        ("subcategory_name" -> rs.string("subcategory_name")) ~ ("category_id" -> rs.string("category_id")) ~
        ("category_name" -> rs.string("category_name")) ~ ("summary" -> rs.string("summary")) ~
        ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency")) ~ ("rating" -> ("id" -> rs.string("sub_cat_id")) ~ ("current" -> rs.string("average_rating")) ~ ("max" -> rs.string("max_rating")) ~
          ("totalRate" -> rs.string("no_of_rating")))
    }

    Drive.register("typeS", sql, map)
  }

  def typeInfoType_D {
    /*val sql = """SELECT a.id subcategory_id,
				| a.name subcategory_name,
				| a.core_category_id category_id,
				| b.name category_name
				| FROM core_subcategory a JOIN core_category b ON b.id = a.core_category_id
				| WHERE a.id = """.stripMargin

    val map = (rs: WrappedResultSet) => {
      ("subcategory_id" -> rs.string("subcategory_id")) ~
        ("subcategory_name" -> rs.string("subcategory_name")) ~ ("category_id" -> rs.string("category_id")) ~
        ("category_name" -> rs.string("category_name"))
    }*/

    val sql = """SELECT a.id subcategory_id,
          | a.name subcategory_name,
          | a.core_category_id category_id,
          | b.name category_name,
          | c.Summary summary,
          | c.Description description,
          | c.Important_Columns important_columns,
          | c.Unit_Price unit_price,
          | c.Unit_Currency unit_currency,
          | sub_cat_id,
          | ifnull(average_rating,0) average_rating,
          | ifnull(no_of_rating,0) no_of_rating,
          | '5' max_rating
          | FROM core_subcategory a 
          | JOIN core_category b ON b.id = a.core_category_id
          | JOIN published_subjects c on a.id = c.core_subcategory_id
          | LEFT JOIN (SELECT subcategory_id sub_cat_id, avg(rating) average_rating,
          | Count(*) no_of_rating FROM user_rating GROUP BY subcategory_id) m  ON a.id = m.sub_cat_id
          | WHERE a.id = """.stripMargin

    val map = (rs: WrappedResultSet) => {
      ("subcategory_id" -> rs.string("subcategory_id")) ~
        ("subcategory_name" -> rs.string("subcategory_name")) ~ ("category_id" -> rs.string("category_id")) ~
        ("category_name" -> rs.string("category_name")) ~ ("summary" -> rs.string("summary")) ~
        ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency")) ~ ("rating" -> ("id" -> rs.string("sub_cat_id")) ~ ("current" -> rs.string("average_rating")) ~ ("max" -> rs.string("max_rating")) ~
          ("totalRate" -> rs.string("no_of_rating")))
    }
    Drive.register("typeD", sql, map)
  }

  def registerCoreCategory {

    var sql = """SELECT id,
    | name,
    | Type,
    | Subject_area  
    | FROM( 
    | SELECT a.id,
    | a.name,
    | 'D' Type,
    | b.name Subject_area 
    | FROM core_subcategory a 
    | JOIN core_category b 
    | on   a.core_category_id = b.id 
    | JOIN published_subjects C
    | ON C.core_subcategory_id = a.id
    | UNION ALL 
    | SELECT  DISTINCT A.id,
    | name,
    | 'S' Type,
    | name Subject_area 
    | FROM core_category A
    | JOIN published_subjects B
    | ON B.core_category_id = A.id 
    | ) C WHERE 1= 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("name" -> rs.string("name")) ~
        ("Type" -> rs.string("Type")) ~ ("Subject_area" -> rs.string("Subject_area"))
    }
    Drive.register("selectField", sql, map);

  }

  def registerScoreCard {

    var sql = """SELECT data_service_id,
			| user_id,
			| core_category_id,
			| core_subcategory_id,
			| metadata_core_id,
			| data_service_name,
			| purchased_date,
			| status,
			| service_type
			| FROM data_services WHERE 1= 1""".stripMargin
    val map = (rs: WrappedResultSet) => {
      ("data_service_id" -> rs.string("data_service_id")) ~ ("user_id" -> rs.string("user_id")) ~
        ("core_category_id" -> rs.string("core_category_id")) ~ ("core_subcategory_id" -> rs.string("core_subcategory_id")) ~
        ("metadata_core_id" -> rs.string("metadata_core_id")) ~ ("data_service_name" -> rs.string("data_service_name")) ~
        ("purchased_date" -> rs.string("purchased_date")) ~ ("status" -> rs.string("status")) ~
        ("service_type" -> rs.string("service_type"))
    }
    Drive.register("scoreCard", sql, map)
  }

  def registerCoreSubCategory {

    var sql = "select id, name from core_subcategory where 1= 1"

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("name" -> rs.string("name"))
    }
    Drive.register("fieldSubCategory", sql, map);
  }

  def registerGetFromCartByIdForSubscriber2 {
    var sql = """SELECT id,
          | user_id,
          | core_category_id,
          | category_name,
          | core_subcategory_id,
          | datasetName,
          | shareType,
          | serviceType,
          | column_delimeter,
          | row_delimeter,
          | filter,
          | selected_columns,
          | ftp_share_path,
          | MarkDelete,
          | CreatedBy,
          | CreationDate,
          | SubscribeOn,
          | IsSubscribe,
          | ApproveStatus,
          | ApprovedOrRejectBy,
          | ApproveOrRejectDate,
          | ApproverOrRejectorID,
          | is_encrypted,
          | is_header,
          | metadata_core_id
          | FROM subscribeCart WHERE 1=1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("user_id" -> rs.string("user_id")) ~ ("core_category_id" -> rs.string("core_category_id")) ~
        ("category_name" -> rs.string("category_name")) ~ ("core_subcategory_id" -> rs.string("core_subcategory_id")) ~ ("datasetName" -> rs.string("datasetName")) ~
        ("shareType" -> rs.string("shareType")) ~ ("serviceType" -> rs.string("serviceType")) ~ ("column_delimeter" -> rs.string("column_delimeter")) ~
        ("row_delimeter" -> rs.string("row_delimeter")) ~ ("filter" -> rs.string("filter")) ~ ("selected_columns" -> rs.string("selected_columns")) ~
        ("ftp_share_path" -> rs.string("ftp_share_path")) ~ ("MarkDelete" -> rs.string("MarkDelete")) ~ ("CreatedBy" -> rs.string("CreatedBy")) ~
        ("CreationDate" -> rs.string("CreationDate")) ~ ("SubscribeOn" -> rs.string("SubscribeOn")) ~ ("IsSubscribe" -> rs.string("IsSubscribe")) ~
        ("ApproveStatus" -> rs.string("ApproveStatus")) ~ ("ApprovedOrRejectBy" -> rs.string("ApprovedOrRejectBy")) ~
        ("ApproveOrRejectDate" -> rs.string("ApproveOrRejectDate")) ~ ("ApproverOrRejectorID" -> rs.string("ApproverOrRejectorID")) ~ ("is_encrypted" -> rs.string("is_encrypted")) ~
        ("is_header" -> rs.string("is_header")) ~ ("metadata_core_id" -> rs.string("metadata_core_id"))
    }

    Drive.register("getFromCartByIdForSubscriber2", sql, map)
  }

  def registerGetFromCartByIdForSubscriber {
    var sql = """SELECT id,
          | user_id,
          | core_category_id,
          | category_name,
          | core_subcategory_id,
          | datasetName,
          | shareType,
          | serviceType,
          | column_delimeter,
          | row_delimeter,
          | filter,
          | selected_columns,
          | ftp_share_path,
          | MarkDelete,
          | CreatedBy,
          | CreationDate,
          | SubscribeOn,
          | IsSubscribe,
          | ApproveStatus,
          | ApprovedOrRejectBy,
          | ApproveOrRejectDate,
          | ApproverOrRejectorID,
          | is_encrypted,
          | is_header,
          | metadata_core_id,
          | Summary summary,
          | Description description,
          | Important_Columns important_columns,
          | Unit_Price unit_price,
          | Unit_Currency unit_currency
          | FROM subscribeCart WHERE 1=1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.int("id")) ~ ("user_id" -> rs.string("user_id")) ~ ("core_category_id" -> rs.string("core_category_id")) ~
        ("category_name" -> rs.string("category_name")) ~ ("core_subcategory_id" -> rs.string("core_subcategory_id")) ~ ("datasetName" -> rs.string("datasetName")) ~
        ("shareType" -> rs.string("shareType")) ~ ("serviceType" -> rs.string("serviceType")) ~ ("column_delimeter" -> rs.string("column_delimeter")) ~
        ("row_delimeter" -> rs.string("row_delimeter")) ~ ("filter" -> rs.string("filter")) ~ ("selected_columns" -> parse(rs.string("selected_columns"))) ~
        ("ftp_share_path" -> rs.string("ftp_share_path")) ~ ("MarkDelete" -> rs.string("MarkDelete")) ~ ("CreatedBy" -> rs.string("CreatedBy")) ~
        ("CreationDate" -> rs.string("CreationDate")) ~ ("SubscribeOn" -> rs.string("SubscribeOn")) ~ ("IsSubscribe" -> rs.string("IsSubscribe")) ~
        ("ApproveStatus" -> rs.string("ApproveStatus")) ~ ("ApprovedOrRejectBy" -> rs.string("ApprovedOrRejectBy")) ~
        ("ApproveOrRejectDate" -> rs.string("ApproveOrRejectDate")) ~ ("ApproverOrRejectorID" -> rs.string("ApproverOrRejectorID")) ~ ("is_encrypted" -> rs.string("is_encrypted")) ~
        ("is_header" -> rs.string("is_header")) ~ ("metadata_core_id" -> rs.string("metadata_core_id")) ~
        ("summary" -> rs.string("summary")) ~ ("description" -> rs.string("description")) ~
        ("important_columns" -> rs.string("important_columns")) ~ ("unit_price" -> rs.string("unit_price")) ~
        ("unit_currency" -> rs.string("unit_currency"))
    }

    Drive.register("getFromCartByIdForSubscriber", sql, map)
  }

  def registerCoreFilteredData {
    /*	var sql = """select data_element_id, data_element_name, length, datatype, category_id, sub_category_id, metadata_core_id
		|  from
		|(select a.id as data_element_id,
		| a.name as data_element_name,
		| b.name as datatype,
		| a.length as length,
		| c.id as metadata_core_id,
		| c.category as category_id,
		| c.sub_category as sub_category_id
		| FROM data_element a
		| JOIN data_type b
		| ON a.data_type = b.id
		| JOIN metadata_core c
		| ON a.metadata_core_id = c.id
		| )X
		| where 1= 1""".stripMargin
		*/
    var sql = """select id, data_element_name, datatype, length, category_id, sub_category_id, metadata_core_id
		|  from
		|(select de.id as id, de.name as data_element_name,
		| dt.name as datatype,
		| de.length as length,
		| mc.id as metadata_core_id,
		| mc.category as category_id,
		| mc.sub_category as sub_category_id
		| from data_element de
		| join data_type dt
		| ON de.data_type = dt.id
		| join metadata_core mc 
		| on de.metadata_core_id = mc.id
		| )X
		| where 1= 1""".stripMargin

    /* var sql = """select id, name, length, data_type, category_id, sub_category_id
		|  from
		|(select de.id as id, de.name as name,
		| de.length as length,
		| de.data_type as data_type,
		| mc.id as metadata_core_id,
		| mc.category as category_id,
		| mc.sub_category as sub_category_id
		| from data_element de
		| join metadata_core mc
		| on de.metadata_core_id = mc.id
		| )X
		| where 1= 1""".stripMargin
		*/
    val map = (rs: WrappedResultSet) => {

      ("id" -> rs.string("id")) ~ ("metadata_core_id" -> rs.string("metadata_core_id")) ~
        ("name" -> rs.string("data_element_name")) ~ ("length" -> rs.string("length")) ~
        ("data_type" -> rs.string("datatype"))

    }
    Drive.register("fieldData", sql, map)
  }

  def registerFilteredDataServices {

    /*
		var sql = """SELECT a.core_category_id,
		| b.name core_category,
		| a.core_subcategory_id,
		| c.name core_subcategory,
		| metadata_core_id,
		| purchased_date,
		| status,
		| service_type,
		| start_date,
		| row_delimiter,
		| is_encrypted,
		| is_header,
		| download_url
		| FROM data_services a
		| JOIN core_category b
		| ON a.core_category_id = b.id
		| JOIN core_subcategory c
		| ON a.core_subcategory_id = c.id
		| WHERE 1= 1""".stripMargin
		*/

    var sql = """SELECT data_service_id,
    | a.core_category_id core_category_id,
    | metadata_core_id,
    | b.name Subject_Area,
    | c.name Dataset,
    | concat_ws('-', b.name, c.name) Service_Name,
    | purchased_date Purchased_Date,
    | status Status,
    | service_type Type,
    | start_date Start_Date,
    | row_delimiter Row_delimeter,
    | column_delimiter Col_delimeter,
    | is_header Header,
    | is_encrypted Encryption,
    | '' Fields,
    | file_size_in_bytes File_Size,
    | '50,000' Cost,
    | '10,000' Amnt_Paid,
    | '40,000' Amnt_Due,
    | download_url download_url,
    | data_service_name, 
    | share_type,        
    | is_ftp_share,            
    | is_database_share, 
    | ftp_share_path,    
    | ftp_username,            
    | ftp_password,            
    | database_type,     
    | database_server_ip,
    | database_instance_name,
    | database_username,
    | database_password,
    | expired_date,
    | a.core_subcategory_id Core_SubCat_id,
    | average_rating,
    | no_of_rating,
    | '5' max_rating
    | FROM data_services a 
    | JOIN core_category b ON a.core_category_id = b.id 
    | JOIN core_subcategory c ON a.core_subcategory_id = c.id 
    | LEFT JOIN (SELECT subcategory_id, avg(rating) average_rating,
    | Count(*) no_of_rating FROM user_rating GROUP BY subcategory_id) m  ON a.core_subcategory_id = m.subcategory_id
    | WHERE 1= 1""".stripMargin

    /*var sql = """SELECT data_service_id,
		| a.core_category_id core_category_id,
		| metadata_core_id,
		| b.name Subject_Area,
		| c.name Dataset,
		| concat_ws('-', b.name, c.name) Service_Name,
		| purchased_date Purchased_Date,
		| status Status,
		| service_type Type,
		| start_date Start_Date,
		| row_delimiter Row_delimeter,
		| column_delimiter Col_delimeter,
		| is_header Header,
		| is_encrypted Encryption,
		| '' Fields,
		| file_size_in_bytes File_Size,
		| '50,000' Cost,
		| '10,000' Amnt_Paid,
		| '40,000' Amnt_Due,
		| download_url download_url,
		| data_service_name,
		| share_type,
		| is_ftp_share,
		| is_database_share,
		| ftp_share_path,
		| ftp_username,
		| ftp_password,
		| database_type,
		| database_server_ip,
		| database_instance_name,
		| database_username,
		| database_password
		| FROM data_services a
		| JOIN core_category b ON a.core_category_id = b.id
		| JOIN core_subcategory c ON a.core_subcategory_id = c.id
		| WHERE 1= 1""".stripMargin*/

    val map = (rs: WrappedResultSet) => {

      ("data_service_id" -> rs.string("data_service_id")) ~
        ("core_category_id" -> rs.string("core_category_id")) ~ ("metadata_core_id" -> rs.string("metadata_core_id")) ~
        ("Subject_Area" -> rs.string("Subject_Area")) ~ ("Dataset" -> rs.string("Dataset")) ~ ("Service_Name" -> rs.string("Service_Name")) ~
        ("Purchased_Date" -> rs.string("Purchased_Date")) ~ ("Status" -> rs.string("Status")) ~ ("Type" -> rs.string("Type")) ~
        ("Start_Date" -> rs.string("Start_Date")) ~ ("Row_delimeter" -> rs.string("Row_delimeter")) ~ ("Col_delimeter" -> rs.string("Col_delimeter")) ~
        ("Header" -> rs.string("Header")) ~ ("Encryption" -> rs.string("Encryption")) ~ ("Fields" -> rs.string("Fields")) ~
        ("File_Size" -> rs.string("File_Size")) ~ ("Cost" -> rs.string("Cost")) ~ ("Amnt_Paid" -> rs.string("Amnt_Paid")) ~
        ("Amnt_Due" -> rs.string("Amnt_Due")) ~ ("download_url" -> rs.string("download_url")) ~ ("data_service_name" -> rs.string("data_service_name")) ~
        ("share_type" -> rs.string("share_type")) ~ ("is_ftp_share" -> rs.string("is_ftp_share")) ~
        ("is_database_share" -> rs.string("is_database_share")) ~ ("ftp_share_path" -> rs.string("ftp_share_path")) ~
        ("ftp_username" -> rs.string("ftp_username")) ~ ("ftp_password" -> rs.string("ftp_password")) ~
        ("database_type" -> rs.string("database_type")) ~ ("database_server_ip" -> rs.string("database_server_ip")) ~
        ("database_instance_name" -> rs.string("database_instance_name")) ~ ("database_username" -> rs.string("database_username")) ~
        ("database_password" -> rs.string("database_password")) ~ ("rating" -> ("id" -> rs.string("Core_SubCat_id")) ~ ("current" -> rs.string("average_rating")) ~ ("max" -> rs.string("max_rating")) ~
          ("totalRate" -> rs.string("no_of_rating")))

    }

    Drive.register("dataServiceData", sql, map)
  }

  def registerNotificationLog {

    var sql = """SELECT data_service_id,
					|message,
					|status,
					|created_date
					|FROM user_notification_log
					|WHERE 1= 1""".stripMargin
    val map = (rs: WrappedResultSet) => {
      ("data_service_id" -> rs.string("data_service_id")) ~
        ("message" -> rs.string("message")) ~ ("status" -> rs.string("status")) ~
        ("created_date" -> rs.string("created_date"))
    }
    Drive.register("notificationLog", sql, map)
  }

  def registerAdminPageAlt {
    var sql = """SELECT user_id,
					|data_service_id,
					|a.core_category_id core_category_id,
					|b.name core_category,
					|core_subcategory_id,
					|c.name core_subcategory,
					|metadata_core_id,
					|purchased_date,
					|status,
					|service_type,
					|start_date,
					|row_delimiter,
					|column_delimiter,
					|is_encrypted,
					|is_header,
					|download_url 
					|FROM data_services a 
					|JOIN core_category b ON a.core_category_id = b.id 
					|JOIN core_subcategory c ON a.core_subcategory_id = c.id""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("user_id" -> rs.string("user_id")) ~ ("core_category_id" -> rs.string("core_category_id")) ~
        ("core_category" -> rs.string("core_category")) ~ ("data_service_id" -> rs.string("data_service_id")) ~
        ("core_subcategory_id" -> rs.string("core_subcategory_id")) ~ ("core_subcategory" -> rs.string("core_subcategory")) ~
        ("metadata_core_id" -> rs.string("metadata_core_id")) ~ ("purchased_date" -> rs.string("purchased_date"))
    }
    Drive.register("adminPageAlt", sql, map)
  }

  def registerAdminPage {

    var sql = """SELECT a.user_id user_id, data_service_id,
		| a.core_category_id core_category_id,
		| metadata_core_id,
		| concat_ws('-',b.name,c.name) Service_Name,
		| purchased_date Purchased_Date,
		| status Status,
		| service_type Type,
		| start_date Start_Date,
		| row_delimiter Row_delimeter,
		| column_delimiter Col_delimeter,
		| is_header Header,
		| is_encrypted Encryption,
		| '' Fields,
		| file_size_in_bytes File_Size,
		| '50,000' Cost,
		| '10,000' Amnt_Paid,
		| '40,000' Amnt_Due,
		| download_url download_url,
		| data_service_name,	
		| share_type,		
		| is_ftp_share,		
		| is_database_share,	
		| ftp_share_path,	
		| ftp_username,		
		| ftp_password,		
		| database_type,	
		| database_server_ip,
		| database_instance_name,
		| database_username,
		| database_password,
		| concat_ws(' ',d.first_name,d.middle_name,d.last_name) order_by 
		| FROM data_services a 
		| JOIN core_category b ON a.core_category_id = b.id 
		| JOIN core_subcategory c ON a.core_subcategory_id = c.id 
		| JOIN users d ON d.id = a.user_id""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("user_id" -> rs.string("user_id")) ~ ("data_service_id" -> rs.string("data_service_id")) ~
        ("user_id" -> rs.string("user_id")) ~ ("core_category_id" -> rs.string("core_category_id")) ~
        ("metadata_core_id" -> rs.string("metadata_core_id")) ~ ("Service_Name" -> rs.string("Service_Name")) ~
        ("Purchased_Date" -> rs.string("Purchased_Date")) ~ ("Status" -> rs.string("Status")) ~
        ("Type" -> rs.string("Type")) ~ ("Start_Date" -> rs.string("Start_Date")) ~ ("Row_delimeter" -> rs.string("Row_delimeter")) ~
        ("Col_delimeter" -> rs.string("Col_delimeter")) ~ ("Header" -> rs.string("Header")) ~ ("Encryption" -> rs.string("Encryption")) ~
        ("Fields" -> rs.string("Fields")) ~ ("File_Size" -> rs.string("File_Size")) ~ ("Cost" -> rs.string("Cost")) ~
        ("Amnt_Paid" -> rs.string("Amnt_Paid")) ~ ("Amnt_Due" -> rs.string("Amnt_Due")) ~ ("download_url" -> rs.string("download_url")) ~
        ("data_service_name" -> rs.string("data_service_name")) ~ ("share_type" -> rs.string("share_type")) ~
        ("is_ftp_share" -> rs.string("is_ftp_share")) ~ ("is_database_share" -> rs.string("is_database_share")) ~
        ("ftp_share_path" -> rs.string("ftp_share_path")) ~ ("ftp_username" -> rs.string("ftp_username")) ~
        ("ftp_password" -> rs.string("ftp_password")) ~ ("database_type" -> rs.string("database_type")) ~
        ("database_server_ip" -> rs.string("database_server_ip")) ~ ("database_instance_name" -> rs.string("database_instance_name")) ~
        ("database_username" -> rs.string("database_username")) ~ ("database_password" -> rs.string("database_password")) ~ ("order_by" -> rs.string("order_by"))
    }
    Drive.register("adminPage", sql, map)
  }

  def registerAdminColumnCheck {

    var sql = """SELECT a.data_service_id data_service_id,
					|b.name data_element_name,
					|c.name datatype,
					|b.length length,
					|a.metadata_core_id metadata_core_id 
					|FROM data_services a 
					|JOIN data_element b ON a.data_service_id= b.id 
					|JOIN data_type c ON b.data_type = c.id""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("data_service_id" -> rs.string("data_service_id")) ~ ("data_element_name" -> rs.string("data_element_name")) ~
        ("datatype" -> rs.string("datatype")) ~ ("length" -> rs.string("length")) ~ ("metadata_core_id" -> rs.string("metadata_core_id"))
    }
    Drive.register("adminColumnCheck", sql, map)
  }

  def registerLoginPage {

    /*var sql = """select * from(SELECT count(*) count
					| FROM users u WHERE 1= 1""".stripMargin*/

    var sql = """SELECT  u.id user_id,
        | u.first_name,
        | u.middle_name, 
        | u.last_name,
        | u.user_id user_name,
        | u.PASSWORD PASSWORD,
        | u.mail_id mail_id,
        | u.contact_no contact_no,
        | u.is_admin is_admin,
        | u.is_subscriber is_subscriber,
        | u.is_publisher is_publisher,
        | u.is_searcher is_searcher,
        | u.is_ingester is_ingester,
        | s.SandboxID sandBoxID 
        | FROM users u 
        | LEFT OUTER JOIN  
        | sandbox s ON  u.user_id=s.UserID 
        | WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("user_id" -> rs.string("user_id")) ~ ("first_name" -> rs.string("first_name")) ~
        ("middle_name" -> rs.string("middle_name")) ~ ("last_name" -> rs.string("last_name")) ~
        ("user_name" -> rs.string("user_name")) ~ ("password" -> rs.string("password")) ~
        ("mail_id" -> rs.string("mail_id")) ~ ("contact_no" -> rs.string("contact_no")) ~
        ("is_admin" -> rs.string("is_admin")) ~ ("is_subscriber" -> rs.string("is_subscriber")) ~
        ("is_publisher" -> rs.string("is_publisher")) ~ ("is_searcher" -> rs.string("is_searcher")) ~
        ("is_ingester" -> rs.string("is_ingester")) ~ ("sandBoxID" -> rs.string("sandBoxID"))
    }
    Drive.register("loginPage", sql, map)
  }

  def registerGroupUserMapDetails {
    val sql = """SELECT Id id,
          | UserId userId,
          | GroupId groupId,
          | MarkDelete markDelete,
          | CreatedBy createdBy,
          | CreationDate creationDate,
          | ModifiedBy modifiedBy,
          | ModificationDate modificationDate
          | FROM groupUserMap
          | WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.string("id")) ~ ("userId" -> rs.string("userId")) ~
        ("groupId" -> rs.string("groupId")) ~ ("markDelete" -> rs.string("markDelete")) ~
        ("createdBy" -> rs.string("createdBy")) ~ ("creationDate" -> rs.string("creationDate")) ~
        ("modifiedBy" -> rs.string("modifiedBy")) ~ ("modificationDate" -> rs.string("modificationDate"))
    }

    Drive.register("groupUserMapDetails", sql, map)
  }

  def registerUserGroupList {
    val sql = """SELECT a.id AS mapId,
          | b.id AS groupid,
          | b.GroupName AS groupName,
          | c.first_name AS firstName,
          | c.middle_name AS middleName,
          | c.last_name AS lastName,
          | c.user_id AS loginId,
          | c.id AS userId 
          | FROM groupUserMap a, tGroup b, users c 
          | WHERE a.GroupId = b.id 
          | AND a.UserId=c.id 
          | AND a.MarkDelete=0 
          | AND b.MarkDelete=0""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("mapId" -> rs.string("mapId")) ~ ("groupid" -> rs.string("groupid")) ~ ("groupName" -> rs.string("groupName")) ~ ("firstName" -> rs.string("firstName")) ~
        ("middleName" -> rs.string("middleName")) ~ ("lastName" -> rs.string("lastName")) ~ ("loginId" -> rs.string("loginId")) ~ ("userId" -> rs.string("userId"))
    }

    Drive.register("getUserGroupList", sql, map)
  }

  def registerSubCategoryGroupList {
    val sql = """SELECT a.id mapId, 
          | b.id AS groupid,
          | b.GroupName groupName,
          | c.name AS subCategoryName, 
          | c.id AS subcategoryid
          | FROM groupSubjectAreaMap a, tGroup b, core_subcategory c
          | WHERE a.GroupId = b.id 
          | AND a.SubCategoryId=c.id 
          | AND a.MarkDelete=0 
          | AND b.MarkDelete=0""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("mapId" -> rs.string("mapId")) ~ ("groupid" -> rs.string("groupid")) ~ ("groupName" -> rs.string("groupName")) ~ ("subCategoryName" -> rs.string("subCategoryName")) ~
        ("subcategoryid" -> rs.string("subcategoryid"))
    }

    Drive.register("getSubCategoryGroupList", sql, map)
  }

  def registerGroupSubjectAreaMapDetails {
    val sql = """SELECT Id id,
          | SubCategoryId subCategoryId,
          | GroupId groupId,
          | MarkDelete markDelete,
          | CreatedBy createdBy,
          | CreationDate creationDate,
          | ModifiedBy modifiedBy,
          | ModificationDate modificationDate
          | FROM groupSubjectAreaMap
          | WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.string("id")) ~ ("subCategoryId" -> rs.string("subCategoryId")) ~
        ("groupId" -> rs.string("groupId")) ~ ("markDelete" -> rs.string("markDelete")) ~
        ("createdBy" -> rs.string("createdBy")) ~ ("creationDate" -> rs.string("creationDate")) ~
        ("modifiedBy" -> rs.string("modifiedBy")) ~ ("modificationDate" -> rs.string("modificationDate"))
    }

    Drive.register("groupSubjectAreaMapDetails", sql, map)
  }

  def registerQuickView {

    val sql = """SELECT hive_database_name, hbase_table
					| FROM metadata_core 
					| WHERE 1= 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("hive_database_name" -> rs.string("hive_database_name")) ~ ("hbase_table" -> rs.string("hbase_table"))
    }
    Drive.register("quickView", sql, map)
  }

  def registerGetAppConfigData {
    val sql = """SELECT id,
          | AppKey appkey,
          | AppValue appvalue,
          | MarkDelete markDelete
          | FROM appConfig
          | WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("id" -> rs.string("id")) ~ ("appkey" -> rs.string("appkey")) ~ ("appvalue" -> rs.string("appvalue")) ~ ("markDelete" -> rs.string("markDelete"))
    }

    Drive.register("getAppConfigData", sql, map)
  }

  def registerDownloadData {
    val sql = """SELECT hive_database_name, hbase_table
					| FROM metadata_core 
					| WHERE 1= 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("hive_database_name" -> rs.string("hive_database_name")) ~ ("hbase_table" -> rs.string("hbase_table"))
    }

    Drive.register("downloadData", sql, map)
  }

  def registerPieChart {

    val sql = "select service_type Type, count(*) count from data_services where 1=1"

    val map = (rs: WrappedResultSet) => {
      ("label" -> rs.string("Type")) ~ ("value" -> rs.string("count"))
    }
    Drive.register("pieChart", sql, map)
  }

  def registerBarChart {

    val sql = """SELECT data_service_name, COUNT(*) count
		| FROM data_services A
		| JOIN order_download_history B ON A.data_service_id = B.data_service_id
		| WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("name" -> rs.string("data_service_name")) ~ ("downloaded" -> rs.string("count"))
    }
    Drive.register("barChart", sql, map)
  }

  def registerGatherDownloadHistory {

    val sql = """SELECT A.data_service_id data_service_id,
		| A.data_service_name data_service_name,
		| download_details,
		| download_status,
		| B.created_date created_date
		| FROM data_services A 
		| JOIN order_download_history B
		| ON A.data_service_id = B.data_service_id
		| WHERE 1 = 1""".stripMargin

    val map = (rs: WrappedResultSet) => {
      ("data_service_id" -> rs.string("data_service_id")) ~ ("data_service_name" -> rs.string("data_service_name")) ~
        ("download_details" -> rs.string("download_details")) ~ ("download_status" -> rs.string("download_status")) ~
        ("created_date" -> rs.string("created_date"))
    }

    Drive.register("getDownloadHistory", sql, map)
  }

}
class RegisterReport {
}
