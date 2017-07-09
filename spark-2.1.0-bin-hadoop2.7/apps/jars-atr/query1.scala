import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.StringBuilder

//warm up
:load ./apps/jars-atr/query1-debug.scala

object Query1Parquet {

  def execute(spark:SparkSession, id: Int = 1, doNullOutput: Boolean = false) {

    val hdfs = ""
    val crail = "crail://flex11-40g0:9060"
    val fs = crail
    val BASE_DATA_PATH=fs+"/sql/dnb/"
    val DUNS_KEYS=fs+"/sql/dnb/queryKeys"+id+".parquet"
    val LINK_TABLE=fs+"/sql/dnb/DNB_TO_ENTERPRISE_ROWID"
    val RESULT_PATH=fs+"/sql/dnb/databses/"
    val RESULT_NAME="Query1Result"

    //spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

    val start:Long = System.nanoTime()

    val keyDF = spark.read.parquet(DUNS_KEYS)
    keyDF.createOrReplaceTempView("DUNS")

    val linkDF = spark.read.parquet(LINK_TABLE)
    linkDF.createOrReplaceTempView("LINK")

    case class DataTable(tabName: String, cols: Array[String])

    val dataTables: Array[DataTable] = Array(
      DataTable("GARTNER_ESTIMATES_CALCULATED_ROWID",
        Array("ENTERPRISE_ID",
          "ENTERPRISE_NAME",
          "REVENUE_SIZE_TH_USD",
          "GARTNER_INDUSTRY",
          "IT_SPENT_TH_USD",
          "IT_SPENT_PRC",
          "IT_SPENT_OPEX_TH_USD",
          "IT_SPENT_OPEX_PRC",
          "EMPLOYEES",
          "IT_SPENT_PER_EMPLOYEE_TH_USD",
          "IT_SPENT_PER_EMPLOYEE_TOTAL_TH_USD",
          "ROWID"
        )),
      DataTable("PEERS_ROWID",
        Array("ENTERPRISE_ID",
          "PEER_ENTERPRISE_ID",
          "CRT_BY",
          "CRT_TSTAMP",
          "ROWID"
        )),
      DataTable("BMSIW_CONTRACTS_PLUS_ROWID",
        Array("PROD_DIV_GRP_NM",
          "PROD_DIV_SUB_SUB_GRP_NM",
          "CRT_TSTAMP",
          "CRT_BY",
          "UPD_TSTAMP",
          "UPD_BY",
          "COVFLAG",
          "WORKNUMBER",
          "COMPANYCODE",
          "STATUS",
          "SERVICETYPE",
          "BUSINESSDESC",
          "PROJECTDESC",
          "TITLE",
          "OPPORTUNITYNO",
          "OFFERINGDESC",
          "CONTRACTNUMBER",
          "LEGALCONNUM",
          "CONTRACTSTATUS",
          "CHANNELINDICATOR",
          "MANAGERCONTACT",
          "MANAGERUID",
          "CONTRACTDESC",
          "CONTRACTTITLE",
          "STARTDATE",
          "ENDDATE",
          "PROJECTREVENUE",
          "CONTRSIGNEDDATE",
          "CONTRBEGINDATE",
          "ENTERPRISE_ID",
          "OFFERINGID",
          "COUNTRY",
          "CUST_NUM",
          "ROWID"
        )),

      DataTable("IBM_REVENUE_UNIFIED_ROWID",
        Array("YEAR_NUM",
          "IBM_TOTAL",
          "TOTAL_STG",
          "TOTAL_SWG",
          "TOTAL_IGF",
          "TOTAL_GTS",
          "TOTAL_GBS",
          "TOTAL_SVCS",
          "AMSSO_REV",
          "CNSI_REV",
          "GPS_REV",
          "ITS_REV",
          "MA_REV",
          "SO_REV",
          "ENTERPRISE_ID",
          "CRT_BY",
          "UPD_TSTAMP"
        )),

      DataTable("IBM_WALLET_SHARE_UNIFIED_ROWID",
        Array("ENTERPRISE_ID",
          "OP_HW_TOTAL_SERVED_09",
          "OP_HW_TOTAL_SERVED_10",
          "OP_HW_TOTAL_SERVED_11",
          "OP_HW_TOTAL_SERVED_12",
          "OP_HW_TOTAL_SERVED_13",
          "OP_SV_TOTAL_SERVED_09",
          "OP_SV_TOTAL_SERVED_10",
          "OP_SV_TOTAL_SERVED_11",
          "OP_SV_TOTAL_SERVED_12",
          "OP_SV_TOTAL_SERVED_13",
          "OP_SW_TOTAL_SERVED_09",
          "OP_SW_TOTAL_SERVED_10",
          "OP_SW_TOTAL_SERVED_11",
          "OP_SW_TOTAL_SERVED_12",
          "OP_SW_TOTAL_SERVED_13",
          "OP_IBM_TOTAL_SERVED_09",
          "OP_IBM_TOTAL_SERVED_10",
          "OP_IBM_TOTAL_SERVED_11",
          "OP_IBM_TOTAL_SERVED_12",
          "OP_IBM_TOTAL_SERVED_13",
          "OP_TOTAL_09",
          "OP_TOTAL_10",
          "OP_TOTAL_11",
          "OP_TOTAL_12",
          "OP_TOTAL_13",
          "RV_IBM_TOTAL_07",
          "RV_IBM_TOTAL_08",
          "RV_IBM_TOTAL_09",
          "RV_IBM_TOTAL_10",
          "RV_IBM_TOTAL_11",
          "RV_HW_TOTAL_07",
          "RV_HW_TOTAL_08",
          "RV_HW_TOTAL_09",
          "RV_HW_TOTAL_10",
          "RV_HW_TOTAL_11",
          "RV_IGF_TOTAL_07",
          "RV_IGF_TOTAL_08",
          "RV_IGF_TOTAL_09",
          "RV_IGF_TOTAL_10",
          "RV_IGF_TOTAL_11",
          "RV_MISC_TOTAL_07",
          "RV_MISC_TOTAL_08",
          "RV_MISC_TOTAL_09",
          "RV_MISC_TOTAL_10",
          "RV_MISC_TOTAL_11",
          "RV_SV_TOTAL_07",
          "RV_SV_TOTAL_10",
          "RV_SW_TOTAL_09",
          "ROWID"
        )),

      DataTable("COMP_INSTALL_BASE_BY_HG_ROWID",
        Array("ENTERPRISE_ID",
          "IND_BY_OTHERS_ID",
          "GEO_ID",
          "COUNTRY_ISO",
          "SUPPLIER_NAME",
          "DATA_SOURCE_ID",
          "DUNS_NO",
          "BUSINESS_NAME",
          "PRODUCT_NAME",
          "DATE_FIRST_VERIFIED",
          "DATE_LAST_VERIFIED",
          "INTENSITY",
          "HG_CATEGORY_1",
          "HG_CATEGORY_2",
          "TRADESTYLE_NAME",
          "CARRIER_ROUTE_CODE",
          "SMSA_CODE",
          "SALES_VOLUME_LOCATION",
          "SALES_VOLUME_LOCATION_CODE",
          "SALES_VOLUME_US",
          "SALES_VOLUME_US_CODE",
          "EMPLOYEES_TOTAL",
          "EMPLOYEES_TOTAL_CODE",
          "EMPLOYEES_HERE",
          "EMPLOYEES_HERE_CODE",
          "STATUS_INDICATOR",
          "MANUFACTURING_INDICATOR",
          "POPULATION_CODE",
          "SIC2_DESCRIPTION",
          "SIC3_DESCRIPTION",
          "SIC4_DESCRIPTION",
          "HG_CATEGORY",
          "ROWID"
        )),

      DataTable("FINANCIAL_EXTENDED_V4_ROWID",
        Array("ENTERPRISE_ID",
          "PERIOD",
          "PERIODTYPENAME",
          "PERIODTYPEABBREVIATION",
          "PERIODENDDATE",
          "FISCALQUARTER",
          "FISCALYEAR",
          "REPORTEDCURRENCY",
          "CURRENCYRATECLOSE",
          "RESTATEMENTTYPENAME",
          "LATESTPERIODFLAG",
          "TOTAL_REVENUE_LOC",
          "TOTAL_REVENUE_USD",
          "TOTAL_ASSETS_LOC",
          "TOTAL_ASSETS_USD",
          "COGS_LOC",
          "COGS_USD",
          "SGNA_LOC",
          "SGNA_USD",
          "EBITDA_LOC",
          "EBITDA_USD",
          "OPERATING_INCOME_LOC",
          "OPERATING_INCOME_USD",
          "NET_INCOME_LOC",
          "NET_INCOME_USD",
          "NPPE_LOC",
          "NPPE_USD",
          "GROSS_PROFIT_LOC",
          "GROSS_PROFIT_USD",
          "INVENTORY_LOC",
          "INVENTORY_USD",
          "RND_EXPENSES_LOC",
          "RND_EXPENSES_USD",
          "TOTAL_OPERATING_EXPENSES_LOC",
          "TOTAL_OPERATING_EXPENSES_USD",
          "EBIT_LOC",
          "EBIT_USD",
          "CASH_FROM_OPERATIONS_LOC",
          "CASH_FROM_OPERATIONS_USD",
          "NET_CAPEX_LOC",
          "NET_CAPEX_USD",
          "CAPEX_LOC",
          "CAPEX_USD",
          "TOTAL_DEBT_LOC",
          "TOTAL_DEBT_USD",
          "TOTAL_LIABILITIES_AND_EQUITY_LOC",
          "TOTAL_LIABILITIES_AND_EQUITY_USD",
          "NET_LOANS_LOC",
          "NET_LOANS_USD",
          "NET_INTEREST_INCOME_LOC",
          "NET_INTEREST_INCOME_USD",
          "TOTAL_NON_INTEREST_INCOME_LOC",
          "TOTAL_NON_INTEREST_INCOME_USD",
          "TOTAL_OTHER_NON_INTEREST_INCOME_LOC",
          "TOTAL_OTHER_NON_INTEREST_INCOME_USD",
          "TOTAL_INTEREST_INCOME_LOC",
          "TOTAL_INTEREST_INCOME_USD",
          "ALLOWANCE_FOR_LOAN_LOSSES_LOC",
          "ALLOWANCE_FOR_LOAN_LOSSES_USD",
          "CASH_AND_EQUIVALENTS_LOC",
          "CASH_AND_EQUIVALENTS_USD",
          "NON_INTEREST_EXPENSE_LOC",
          "NON_INTEREST_EXPENSE_USD",
          "POLICY_ACQUISITION_AND_UNDERWRITING_COSTS_LOC",
          "POLICY_ACQUISITION_AND_UNDERWRITING_COSTS_USD",
          "POLICY_BENEFITS_LOC",
          "POLICY_BENEFITS_USD",
          "PREMIUMS_AND_ANNUITY_REVENUES_LOC",
          "PREMIUMS_AND_ANNUITY_REVENUES_USD",
          "GROSS_WRITTEN_PREMIUMS_LOC",
          "GROSS_WRITTEN_PREMIUMS_USD",
          "CEDED_WRITTEN_PREMIUMS_LOC",
          "CEDED_WRITTEN_PREMIUMS_USD",
          "NET_WRITTEN_PREMIUMS_LOC",
          "NET_WRITTEN_PREMIUMS_USD",
          "INSURANCE_REVENUES_LOC",
          "INSURANCE_REVENUES_USD",
          "RETURN_ON_ASSETS",
          "FIXED_ASSET_UTILIZATION",
          "GMROII",
          "COGS_PER_REVENUE",
          "RND_PER_REVENUE",
          "SGNA_PER_REVENUE",
          "TOTAL_EXPENSE_YOY_GROWTH",
          "DEGREE_OF_OPERATING_LEVERAGE",
          "GROSS_PROFIT_MARGIN",
          "OPERATING_MARGIN",
          "EBITDA_MARGIN",
          "AVG_DAYS_SALES_OUTSTANDING",
          "AVG_DAYS_PAYABLE_OUTSTANDING",
          "DAYS_INVENTORY_OUTSTANDING",
          "CASH_CONVERSION_CYCLE",
          "CAPEX_COVERAGE",
          "CF_TO_DEBT",
          "CURRENT_RATIO",
          "QUICK_RATIO",
          "DEPT_TO_CAPITAL_RATIO",
          "DILUTED_EPS_BEFORE_EXTRA_3YR_CAGR",
          "NON_PERFORMING_LOANS_PERCENT",
          "LOANS_TO_ASSETS",
          "EXPENSES_TO_ASSETS",
          "EXPENSES_TO_OPERATING_INCOME",
          "INTEREST_INCOME_LEVEL",
          "NON_INTEREST_INCOME_LEVEL",
          "GROSS_INTEREST_MARGIN",
          "NET_INTEREST_MARGIN",
          "TOTAL_RESERVE_CAPITAL",
          "RESERVE_TO_LOANS",
          "GROSS_LOANS_TO_DEPOSITS",
          "FREE_CASH_FLOW_PER_REVENUE",
          "SGNA_COGS_TO_ASSETS",
          "SGNA_COGS_TO_EBIT",
          "OP_INCOME_TO_ASSETS",
          "OPERATING_EXPENSES_MARGIN",
          "NET_INCOME_MARGIN",
          "TOTAL_REVENUE_GROWTH_1YR",
          "CAPITAL_EXPENDITURE_GROWTH_1YR",
          "TOTAL_ASSETS_GROWTH_1YR",
          "NON_INTEREST_INCOME_GROWTH_1YR",
          "NON_INTEREST_EXPENSE_BY_AVERAGE_ASSETS",
          "REVENUE_PER_EMPLOYEE_USD",
          "POLICY_ACQ_UNDERWRT_COSTS_BY_REVENUE",
          "CASH_FROM_OPERATIONS_BY_REVENUE",
          "PREMIUM_GROWTH_1YR",
          "RETURN_ON_EQUITY",
          "RETURN_ON_CAPITAL",
          "NET_WRIT_PREM_TO_GROSS_WRIT_PREM",
          "NET_LOSSES_TO_NET_EARNED_PREMIUMS",
          "TOTAL_DEBT_BY_TOTAL_CAPITAL",
          "CEDED_BY_GROSS_WRITTEN_PREMIUMS",
          "NET_PREMIUMS_EARNED_BY_TOTAL_REVENUE",
          "POLICY_AQUISITION_COST_BY_NET_PREMIUMS_EARNED",
          "POLICY_BENEFITS_BY_NET_PREMIUMS_EARNED",
          "POLICY_AQUISITION_AND_POLICY_BENEFITS_BY_NET_PREMIUMS_EARNED",
          "TIER1_CAPITAL_RATIO",
          "CHANGE_IN_NET_WORKING_CAPITAL",
          "CRT_TSTAMP",
          "CRT_BY",
          "UPD_TSTAMP",
          "UPD_BY",
          "SOURCE",
          "ROWID",
          "FULL_TIME_EMPLOYEES"
        )),

      DataTable("FIRMOGRAPHICS_DETAILS_ROWID",
        Array("ENTERPRISE_NAME",
          "TIMEST",
          "ENTERPRISE_ID",
          "DATA_SOURCE_ID",
          "SOURCE_FULL_NAME",
          "ACCOUNT_ID",
          "ACCOUNT_NAME",
          "YEAR_NUM",
          "FIRM_PRIORITY",
          "EMPLOYEES",
          "EMPLOYEES_IT",
          "PR_ADDR",
          "PR_ZIPCODE",
          "CITY",
          "MAIN_PH_NUM",
          "MAIN_FAX_PH_NUM",
          "WWW",
          "IBM_SECTOR",
          "IBM_CODE",
          "IBM_CLASS",
          "IBM_INDUSTRY_NAME",
          "IBM_SUBINDUSTRY",
          "OTHER_IBM_SECTOR",
          "OTHER_SECTOR",
          "OTHER_INDUSTRY",
          "OTHER_CODE",
          "SIC",
          "CONTRACTS",
          "OPPORTUNITIES",
          "ACCT_STATUS",
          "ACCT_TYPE",
          "ACCT_ACTIVE",
          "ACCT_OWNER",
          "CONTACTS",
          "STATE",
          "PR_STATE",
          "ROWID"
        )),

      DataTable("STRATEGIC_INTENT_ROWID",
        Array("DOCUMENT_ID",
          "ENTERPRISE_ID",
          "CTRY",
          "SOURCE",
          "SUBSOURCE",
          "DOCUMENT_DATE",
          "DOCUMENT_SOURCE_KEY",
          "COMPANY_NAME",
          "TYPE",
          "HEADLINE",
          "SITUATION",
          "CATEGORY",
          "LINK",
          "METHOD",
          "SIGNIFICANCE",
          "LAST_UPDATE",
          "LANG",
          "TEASER",
          "INGESTED_TIME"
        )),

      DataTable("SERVICE_CONTRACTS_ROWID",
        Array("ENTERPRISE_ID",
          "ENTERPRISE_NAME",
          "DATA_SOURCE_ID",
          "SOURCE_FULL_NAME",
          "WORKNUMBER",
          "COMPANYCODE",
          "STATUS",
          "STARTDATE",
          "ENDDATE",
          "SERVICEDESC",
          "BUSINESSDESC",
          "PROJECTDESC",
          "TITLE",
          "PROD_DIV_GRP_NM",
          "BMDIV_NM",
          "OPPORTUNITYNO",
          "OFFERINGDESC",
          "MANAGERCONTACT",
          "MANAGERUID",
          "CONTRACTNUMBER",
          "LEGALCONNUM",
          "SOURCE",
          "CONTRACTSTATUS",
          "CONTRACTDESC",
          "CONTRACTTITLE",
          "CHANNEL",
          "CONTRBEGINDATE",
          "SALESPRICE",
          "PROJECTREVENUE",
          "COVFLAG",
          "COMPANYCODE_UC"
        )))


    val linkQuery = """
            select distinct LINK.ENTERPRISE_ID from DUNS 
            join LINK on DUNS.DUNS_NO=LINK.DUNS_NO
                    """

    val eidDF=spark.sql(linkQuery)
    eidDF.persist(StorageLevel.MEMORY_ONLY)
    eidDF.cache()
    eidDF.createOrReplaceTempView("EID")

    var i = 0
    val runTimes = new Array[Long](dataTables.length)

    for(tab <- dataTables) {
      val ss = System.nanoTime()

      val fname = BASE_DATA_PATH + tab.tabName
      val df = spark.read.parquet(fname)
      df.createOrReplaceTempView("DATA")

      val cols = tab.cols.map{unqualified => "DATA." + unqualified}.mkString(",")
      val queryBuilder: StringBuilder = new StringBuilder
      queryBuilder.append("select ")
      queryBuilder.append(cols)
      queryBuilder.append(" from EID join DATA")
      queryBuilder.append(" on EID.ENTERPRISE_ID=DATA.ENTERPRISE_ID")
      val query=queryBuilder.toString()

      System.out.println(s"***query:\n\n$query\n")

      val data = spark.sql(query)
      data.explain()
      val tabName = RESULT_PATH + RESULT_NAME + i.toString

      System.out.println("Saving file at : " +  tabName)
      if(doNullOutput){
        data.write.format("org.apache.spark.sql.execution.datasources.atr.AtrFileFormat").save(tabName)
      } else {
        data.write.format("parquet").mode(SaveMode.Overwrite).save(tabName)
      }

      runTimes(i) = System.nanoTime() - ss
      System.out.println("\t [*** exe took : " + runTimes(i).toFloat/1000000 + " msec ***]")
      i = i + 1
    }
    val totalTime:Long = System.nanoTime() - start

    System.out.println("----------------------------------------")
    for(i <- runTimes){
      System.out.println("\texe : " + i.toFloat/1000000 + " msec")
    }
    System.out.println("----------------------------------------")
    System.out.println(" Total time was : " + totalTime.toFloat/1000000000 + " sec")
    System.out.println("----------------------------------------")

    /*
 * real	9m45.612s
 * user	2m2.835s
 * sys	0m12.366s
 */

    //sc.stop
  }
}
