import org.apache.spark.sql._
import org.apache.spark.sql.types._


class TpcdsSchema() {
	/* Store sales start schema tables (done = +)
	    DATE_DIM +? 
	    TIME_DIM +
	    CUSTOMER +
	    CUSTOMER_ADDRESS +
	    CUSTOMER_DEMOGRAPHICS +
	    HOUSEHOLD_DEMOGRAPHICS +
	    ITEM +
	    PROMOTION +
	    STORE +?
	    STORE_SALES +
            INVENTORY
	*/
	var schema = scala.collection.mutable.Map[String, org.apache.spark.sql.types.StructType]();

	/*
	    TIME_DIM
	Column	  Datatype	NULLs   PrimaryKey      ForeignKey
	t_time_sk       Identifier      N       Y
	t_time_id (B)   char(16)	N
	t_time	  Integer
	t_hour	  Integer
	t_minute	Integer
	t_second	Integer
	t_am_pm	 char(2)
	t_shift	 char(20)
	t_sub_shift     char(20)
	t_meal_time     char(20)
	*/
	schema("time_dim") = org.apache.spark.sql.types.StructType(Array(
		org.apache.spark.sql.types.StructField("t_time_sk",	org.apache.spark.sql.types.IntegerType,     true),
		org.apache.spark.sql.types.StructField("t_time_id",	org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("t_time",	   org.apache.spark.sql.types.IntegerType,    true),
		org.apache.spark.sql.types.StructField("t_hour",	   org.apache.spark.sql.types.IntegerType,    true),
		org.apache.spark.sql.types.StructField("t_minute",	 org.apache.spark.sql.types.IntegerType,    true),
		org.apache.spark.sql.types.StructField("t_second",	 org.apache.spark.sql.types.IntegerType,    true),
		org.apache.spark.sql.types.StructField("t_am_pm",	  org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("t_shift",	  org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("t_sub_shift",      org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("t_meal_time",      org.apache.spark.sql.types.StringType,     true)
	        ))
	/*
	      DATE_DIM
	*/
	schema("date_dim") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("d_date_sk",	org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_date_id",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_date",	   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_month_seq",      org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_week_seq",       org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_quarter_seq",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_year",	   org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_dow",	    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_moy",	    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_dom",	    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_qoy",	    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_fy_year",	org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_fy_quarter_seq", org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_fy_week_seq",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_day_name",       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_quarter_name",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_holiday",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_weekend",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_following_holiday",      org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_first_dom",      org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_last_dom",       org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_same_day_ly",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_same_day_lq",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("d_current_day",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_current_week",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_current_month",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_current_quarter",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("d_current_year",   org.apache.spark.sql.types.StringType,     true)::
		Nil)
	
	/*
	    CUSTOMER
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	c_customer_sk	   identifier      N       Y
	c_customer_id (B)       char(16)	N
	c_current_cdemo_sk      identifier			      cd_demo_sk
	c_current_hdemo_sk      identifier			      hd_demo_sk
	c_current_addr_sk       identifier			      ca_addres_sk
	c_first_shipto_date_sk  identifier			      d_date_sk
	c_first_sales_date_sk   identifier			      d_date_sk
	c_salutation	    char(10)
	c_first_name	    char(20)
	c_last_name	     char(30)
	c_preferred_cust_flag   char(1)
	c_birth_day	     integer
	c_birth_month	   integer
	c_birth_year	    integer
	c_birth_country	 varchar(20)
	c_login		 char(13)
	c_email_address	 char(50)
	c_last_review_date_sk   identifier
	*/
	schema("customer") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("c_customer_sk",    org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_customer_id",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_current_cdemo_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_current_hdemo_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_current_addr_sk",	org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_first_shipto_date_sk",   org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_first_sales_date_sk",    org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("c_salutation",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_first_name",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_last_name",      org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_preferred_cust_flag",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_birth_day",      org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("c_birth_month",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("c_birth_year",     org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("c_birth_country",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_login",	 	 org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_email_address",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("c_last_review_date_sk",    org.apache.spark.sql.types.IntegerType,     true)::
		Nil)
	
	/*
	    CUSTOMER_ADDRESS
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	ca_address_sk	   identifier      N       Y
	ca_address_id (B)       char(16)	N
	ca_street_number	char(10)
	ca_street_name	  varchar(60)
	ca_street_type	  char(15)
	ca_suite_number	 char(10)
	ca_city		 varchar(60)
	ca_county	       varchar(30)
	ca_state		char(2)
	ca_zip		  char(10)
	ca_country	      varchar(20)
	ca_gmt_offset	   decimal(5,2)
	ca_location_type	char(20)
	*/
	schema("customer_address") = org.apache.spark.sql.types.StructType(Array(
		org.apache.spark.sql.types.StructField("ca_address_sk",    org.apache.spark.sql.types.IntegerType,     true),
		org.apache.spark.sql.types.StructField("ca_address_id",    org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_street_number", org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_street_name",   org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_street_type",   org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_suite_number",  org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_city",	  org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_county",	org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_state",	 org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_zip",	   org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_country",       org.apache.spark.sql.types.StringType,     true),
		org.apache.spark.sql.types.StructField("ca_gmt_offset",    org.apache.spark.sql.types.DecimalType(15,2),  true),
		org.apache.spark.sql.types.StructField("ca_location_type", org.apache.spark.sql.types.StringType,     true) 
		))
	
	/*
	    CUSTOMER_DEMOGRAPHICS
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	cd_demo_sk	      identifier      N       Y
	cd_gender	       char(1)
	cd_marital_status       char(1)
	cd_education_status     char(20)
	cd_purchase_estimate    integer
	cd_credit_rating	char(10)
	cd_dep_count	    integer
	cd_dep_employed_count   integer
	cd_dep_college_count    integer
	*/
	schema("customer_demographics") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("cd_demo_sk",	       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("cd_gender",		org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("cd_marital_status",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("cd_education_status",      org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("cd_purchase_estimate",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("cd_credit_rating",	 org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("cd_dep_count",	     org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("cd_dep_employed_count",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("cd_dep_college_count",     org.apache.spark.sql.types.IntegerType,    true)::
		Nil)
	
	/*
	    HOUSEHOLD_DEMOGRAPHICS
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	hd_demo_sk	      identifier      N       Y
	hd_income_band_sk       identifier			      ib_income_band_sk
	hd_buy_potential	char(15)
	hd_dep_counti	   integer
	hd_vehicle_count	integer
	*/
	schema("household_demographics") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("hd_demo_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("hd_income_band_sk",	org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("hd_buy_potential", org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("hd_dep_count",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("hd_vehicle_count", org.apache.spark.sql.types.IntegerType,    true)::
		Nil)
	
	/*
	    ITEM
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	i_item_sk	       identifier      N       Y
	i_item_id (B)	   char(16)	N
	i_rec_start_date	date
	i_rec_end_date	  date
	i_item_desc	     varchar(200)
	i_current_price	 decimal(7,2)
	i_wholesale_cost	decimal(7,2)
	i_brand_id	      integer
	i_brand		 char(50)
	i_class_id	      integer
	i_class		 char(50)
	i_category_id	   integer
	i_category	      char(50)
	i_manufact_id	   integer
	i_manufact	      char(50)
	i_size		  char(20)
	i_formulation	   char(20)
	i_color		 char(20)
	i_units		 char(10)
	i_container	     char(10)
	i_manager_id	    integer
	i_product_name	  char(50)
	*/
	schema("item") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("i_item_sk",	org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("i_item_id",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_rec_start_date", org.apache.spark.sql.types.DateType,       true)::
		org.apache.spark.sql.types.StructField("i_rec_end_date",   org.apache.spark.sql.types.DateType,       true)::
		org.apache.spark.sql.types.StructField("i_item_desc",      org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_current_price",  org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("i_wholesale_cost", org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("i_brand_id",       org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("i_brand"   ,       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_class_id",       org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("i_class",	  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_category_id",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("i_category",       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_manufact_id",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("i_manufact",       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_size",	   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_formulation",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_color",	  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_units",	  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_container",      org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("i_manager_id",     org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("i_product_name",   org.apache.spark.sql.types.StringType,     true)::
		Nil)
	
	/*
	    PROMOTION
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	p_promo_sk	      identifier      N       Y
	p_promo_id (B)	  char(16)	N
	p_start_date_sk	 identifier			      d_date_sk
	p_end_date_sk	   identifier			      d_date_sk
	p_item_sk	       identifier			      i_item_sk
	p_cost		  decimal(15,2)
	p_response_target       integer
	p_promo_name	    char(50)
	p_channel_dmail	 char(1)
	p_channel_email	 char(1)
	p_channel_catalog       char(1)
	p_channel_tv	    char(1)
	p_channel_radio	 char(1)
	p_channel_press	 char(1)
	p_channel_event	 char(1)
	p_channel_demo	  char(1)
	p_channel_details       varchar(100)
	p_purpose	       char(15)
	p_discount_active       char(1)
	*/
	schema("promotion") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("p_promo_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("p_promo_id",       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_start_date_sk",  org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("p_end_date_sk",    org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("p_item_sk",	org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("p_cost",	   org.apache.spark.sql.types.DecimalType(15,2),      true)::
		org.apache.spark.sql.types.StructField("p_response_target",org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("p_promo_name",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_dmail",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_email",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_catalog",org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_tv",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_radio",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_press",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_event",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_demo",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_channel_details",org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_purpose",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("p_discount_active",org.apache.spark.sql.types.StringType,     true)::
		Nil)
	
	/*
	    STORE
	Column		  Datatype	NULLs   PrimaryKey      ForeignKey
	s_store_sk	      identifier
	s_store_id (B)	  char(16)
	s_rec_start_date	date
	s_rec_end_date	  date
	s_closed_date_sk	identifier
	s_store_name	    varchar(50)
	s_number_employees      integer
	s_floor_space	   integer
	s_hours		 char(20)
	s_manager	       varchar(40)
	s_market_id	     integer
	s_geography_class       varchar(100)
	s_market_desc	   varchar(100)
	s_market_manager	varchar(40)
	s_division_id	   integer
	s_division_name	 varchar(50)
	s_company_id	    integer
	s_company_name	  varchar(50)
	s_street_number	 varchar(10)
	s_street_name	   varchar(60)
	s_street_type	   char(15)
	s_suite_number	  char(10)
	s_city		  varchar(60)
	s_county		varchar(30)
	s_state		 char(2)
	s_zip		   char(10)
	s_country	       varchar(20)
	s_gmt_offset	    decimal(5,2)
	s_tax_percentage	decimal(5,2)
	*/
	schema("store") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("s_store_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("s_store_id",       org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_rec_start_date", org.apache.spark.sql.types.DateType,       true)::
		org.apache.spark.sql.types.StructField("s_rec_end_date",   org.apache.spark.sql.types.DateType,       true)::
		org.apache.spark.sql.types.StructField("s_closed_date_sk", org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("s_store_name",     org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_number_employees",       org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("s_floor_space",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("s_hours",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_manager",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_market_id",      org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("s_geography_class",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_market_desc",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_market_manager", org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_division_id",    org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("s_division_name",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_company_id",     org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("s_company_name",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_street_number",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_street_name",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_street_type",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_suite_number",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_city",   org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_county", org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_state",  org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_zip",    org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_country",	org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("s_gmt_offset",     org.apache.spark.sql.types.DecimalType(5,2),       true)::
		org.apache.spark.sql.types.StructField("s_tax_percentage", org.apache.spark.sql.types.DecimalType(5,2),       true)::
		Nil)

	/*
	   STORE_SALES (23 columns)
	Column   		 Datatype	NULLs	PrimaryKey	ForeignKey
	ss_sold_date_sk	 	identifier		   		d_date_sk
	ss_sold_time_sk	 	identifier		     		t_time_sk
	ss_item_sk (1)	  	identifier      N       Y       	i_item_sk,sr_item_sk
	ss_customer_sk	  	identifier		      		c_customer_sk
	ss_cdemo_sk	     	identifier		      		cd_demo_sk
	ss_hdemo_sk    		identifier		      		hd_demo_sk
	ss_addr_sk  		identifier		      		ca_address_sk
	ss_store_sk	   	identifier		      		s_store_sk
	ss_promo_sk    		identifier		      		p_promo_sk
	ss_ticket_number (2)    identifier      N       Y       	sr_ticket_number
	ss_quantity	   	integer
	ss_wholesale_cost       decimal(7,2)
	ss_list_price		decimal(7,2)
	ss_sales_price	  	decimal(7,2)
	ss_ext_discount_amt     decimal(7,2)
	ss_ext_sales_price      decimal(7,2)
	ss_ext_wholesale_cost   decimal(7,2)
	ss_ext_list_price       decimal(7,2)
	ss_ext_tax	      	decimal(7,2)
	ss_coupon_amt	   	decimal(7,2)
	ss_net_paid	     	decimal(7,2)
	ss_net_paid_inc_tax     decimal(7,2)
	ss_net_profit	   	decimal(7,2)
	*/

	schema("store_sales") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("ss_sold_date_sk",  org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_sold_time_sk",  org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_item_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_customer_sk",   org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_cdemo_sk",      org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_hdemo_sk",      org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_addr_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_store_sk",      org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_promo_sk",      org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("ss_ticket_number", org.apache.spark.sql.types.StringType,     true)::
		org.apache.spark.sql.types.StructField("ss_quantity",      org.apache.spark.sql.types.IntegerType,    true)::
		org.apache.spark.sql.types.StructField("ss_wholesale_cost",	org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_list_price",	    org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_sales_price",	   org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_ext_discount_amt",      org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_ext_sales_price",       org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_ext_wholesale_cost",    org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_ext_list_price",	org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_ext_tax",	       org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_coupon_amt",	    org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_net_paid",	      org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_net_paid_inc_tax",      org.apache.spark.sql.types.DecimalType(7,2),       true)::
		org.apache.spark.sql.types.StructField("ss_net_profit",	    org.apache.spark.sql.types.DecimalType(7,2),       true)::
		Nil)

	/*
	    INVENTORY
		Column		  Datatype	NULLs   PrimaryKey      ForeignKey
		inv_date_sk (1)		identifier	N	Y	d_date_sk
		inv_item_sk (2)		identifier	N	Y	i_item_sk
		inv_warehouse_sk (3)	identifier	N	Y	w_warehouse_sk
		inv_quantity_on_hand	integer
	*/	
	schema("inventory") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("inv_date_sk", 		 org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("inv_item_sk", 		 org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("inv_warehouse_sk",       org.apache.spark.sql.types.IntegerType,     true)::
		org.apache.spark.sql.types.StructField("inv_quantity_on_hand",   org.apache.spark.sql.types.IntegerType,     true)::
	        Nil)

	/*
	    WAREHOUSE
		Column		  Datatype	NULLs   PrimaryKey      ForeignKey
		w_warehouse_sk		identifier	N	Y
		w_warehouse_id (B)	char(16)	N	
		w_warehouse_name	varchar(20)	
		w_warehouse_sq_ft	integer
		w_street_number		char(10)
		w_street_name		varchar(60)
		w_street_type		char(15)
		w_suite_number		char(10)
		w_city			varchar(60)
		w_county		varchar(30)
		w_state			char(2)
		w_zip			char(10)
		w_country		varchar(20)
		w_gmt_offset		decimal(5,2)
	*/
	schema("warehouse") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("w_warehouse_sk"		,	org.apache.spark.sql.types.IntegerType     ,	true)::
		org.apache.spark.sql.types.StructField("w_warehouse_id"		,	org.apache.spark.sql.types.StringType      ,	true)::
		org.apache.spark.sql.types.StructField("w_warehouse_name"	, 	org.apache.spark.sql.types.StringType      ,	true)::
		org.apache.spark.sql.types.StructField("w_warehouse_sq_ft"	, 	org.apache.spark.sql.types.IntegerType     , 	true)::
		org.apache.spark.sql.types.StructField("w_street_number"	,  	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_street_name"		, 	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_street_type"		,     	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_suite_number"		,    	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_city" 		, 	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_county"		, 	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_state"		, 	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_zip"			,	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_country"		,	org.apache.spark.sql.types.StringType      , 	true)::
		org.apache.spark.sql.types.StructField("w_gmt_offset"		,	org.apache.spark.sql.types.DecimalType(5,2), 	true)::
	        Nil)

	/*
	    WEB_SALES
	Column 				Datatype	NULLs	PrimaryKey      ForeignKey
	ws_sold_date_sk			identifier				d_date_sk	
	ws_sold_time_sk			identifier				t_time_sk
	ws_ship_date_sk			identifier				d_date_sk
	ws_item_sk (1)			identifier	N	Y		i_item_sk
	ws_bill_customer_sk		identifier				c_customer_sk
	ws_bill_cdemo_sk		identifier				cd_demo_sk
	ws_bill_hdemo_sk		identifier				hd_demo_sk
	ws_bill_addr_sk			identifier				ca_address_sk
	ws_ship_customer_sk		identifier				c_customer_sk
	ws_ship_cdemo_sk		identifier				cd_demo_sk
	ws_ship_hdemo_sk		identifier				hd_demo_sk
	ws_ship_addr_sk			identifier				ca_address_sk
	ws_web_page_sk			identifier				wp_web_page_sk
	ws_web_site_sk			identifier				web_site_sk
	ws_ship_mode_sk			identifier				sm_ship_mode_sk
	ws_warehouse_sk			identifier				w_warehouse_sk
	ws_promo_sk			identifier				p_promo_sk
	ws_order_number (2)		identifier	N	Y	
	ws_quantity			integer
	ws_wholesale_cost		decimal(7,2)
	ws_list_price			decimal(7,2)
	ws_sales_price			decimal(7,2)
	ws_ext_discount_amt		decimal(7,2)
	ws_ext_sales_price		decimal(7,2)
	ws_ext_wholesale_cost		decimal(7,2)
	ws_ext_list_price		decimal(7,2)
	ws_ext_tax			decimal(7,2)
	ws_coupon_amt			decimal(7,2)
	ws_ext_ship_cost		decimal(7,2)
	ws_net_paid			decimal(7,2)
	ws_net_paid_inc_tax		decimal(7,2)
	ws_net_paid_inc_ship		decimal(7,2)
	ws_net_paid_inc_ship_tax	decimal(7,2)
	ws_net_profit			decimal(7,2)
	*/
/*
	schema("web_sales") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("ws_sold_date_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_sold_time_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_date_sk"			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_item_sk"				,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_bill_customer_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_bill_cdemo_sk"			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_bill_hdemo_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_bill_addr_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_customer_sk"			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_cdemo_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_hdemo_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_addr_sk"			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_web_page_sk"				,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_web_site_sk"				,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_ship_mode_sk"			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_warehouse_sk"			,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_promo_sk"				,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_order_number" 			, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("ws_quantity"				,	org.apache.spark.sql.types.IntegerType    	,	true)::
		org.apache.spark.sql.types.StructField("ws_wholesale_cost"			,	org.apache.spark.sql.types.DecimalType(7,2) 	,	true)::
		org.apache.spark.sql.types.StructField("ws_list_price"				,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_sales_price"				,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_discount_amt"			, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_sales_price"			,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_wholesale_cost"			,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_list_price"			, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_tax"				,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_coupon_amt"				,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_ext_ship_cost"			, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_net_paid"				,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_net_paid_inc_tax"			,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_net_paid_inc_ship"			, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_net_paid_inc_ship_tax"		, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("ws_net_profit"				, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
	        Nil)

*/
	/*
	    CATALOG SALES
	Column 				Datatype	NULLs	PrimaryKey      ForeignKey
	cs_sold_date_sk			identifier				d_date_sk
	cs_sold_time_sk			identifier				t_time_sk
	cs_ship_date_sk			identifier				d_date_sk
	cs_bill_customer_sk		identifier				c_customer_sk
	cs_bill_cdemo_sk		identifier				cd_demo_sk
	cs_bill_hdemo_sk 		identifier				hd_demo_sk
	cs_bill_addr_sk			identifier				ca_address_sk
	cs_ship_customer_sk		identifier				c_customer_sk
	cs_ship_cdemo_sk		identifier				cd_demo_sk
	cs_ship_hdemo_sk		identifier				hd_demo_sk
	cs_ship_addr_sk			identifier				ca_address_sk
	cs_call_center_sk		identifier				cc_call_center_sk
	cs_catalog_page_sk		identifier				cp_catalog_page_sk
	cs_ship_mode_sk			identifier				sm_ship_mode_sk
	cs_warehouse_sk			identifier				w_warehouse_sk
	cs_item_sk (1)			identifier	N	Y		i_item_sk
	cs_promo_sk			identifier				p_promo_sk
	cs_order_number (2)		identifier	N	Y
	cs_quantity			integer
	cs_wholesale_cost	 	decimal(7,2)
	cs_list_price 			decimal(7,2)
	cs_sales_price			decimal(7,2)
	cs_ext_discount_amt		decimal(7,2)
	cs_ext_sales_price		decimal(7,2)
	cs_ext_wholesale_cost		decimal(7,2)
	cs_ext_list_price		decimal(7,2)
	cs_ext_tax			decimal(7,2)
	cs_coupon_amt			decimal(7,2)
	cs_ext_ship_cost		decimal(7,2)
	cs_net_paid			decimal(7,2)
	cs_net_paid_inc_tax		decimal(7,2)
	cs_net_paid_inc_ship		decimal(7,2)
	cs_net_paid_inc_ship_tax	decimal(7,2)
	cs_net_profit			decimal(7,2)
	*/
/*
	schema("catalog_sales") = org.apache.spark.sql.types.StructType(
		org.apache.spark.sql.types.StructField("cs_sold_date_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_sold_time_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_date_sk"	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_bill_customer_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_bill_cdemo_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_bill_hdemo_sk" 	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_bill_addr_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_customer_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_cdemo_sk"	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_hdemo_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_addr_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_call_center_sk"	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_catalog_page_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_ship_mode_sk"	,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_warehouse_sk"	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_item_sk" 		,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_promo_sk"		,	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_order_number" 	, 	org.apache.spark.sql.types.IntegerType   	,	true)::
		org.apache.spark.sql.types.StructField("cs_quantity"		,	org.apache.spark.sql.types.IntegerType    	,	true)::
		org.apache.spark.sql.types.StructField("cs_wholesale_cost"	,	org.apache.spark.sql.types.DecimalType(7,2) 	,	true)::
		org.apache.spark.sql.types.StructField("cs_list_price" 		,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_sales_price"		,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_discount_amt"	, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_sales_price"	,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_wholesale_cost"	,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_list_price"	, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_tax"		,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_coupon_amt"		,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_ext_ship_cost"	, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_net_paid"		,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_net_paid_inc_tax"	,	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_net_paid_inc_ship"	, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_net_paid_inc_ship_tax", 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
		org.apache.spark.sql.types.StructField("cs_net_profit" 		, 	org.apache.spark.sql.types.DecimalType(7,2)     ,	true)::
	        Nil)
*/
}
