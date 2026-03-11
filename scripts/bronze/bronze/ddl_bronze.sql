------------------Load for cust_info_table-------------------------------
LOAD DATA INFILE '/var/lib/mysql-files/cust_info.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@cst_id,
 cst_key,
 @cst_firstname,
 @cst_lastname,
 cst_marital_status,
 cst_gndr,
 @cst_create_date)
SET
cst_id = NULLIF(TRIM(@cst_id),''),
cst_firstname = TRIM(@cst_firstname),
cst_lastname = TRIM(@cst_lastname),
cst_create_date = STR_TO_DATE(NULLIF(TRIM(@cst_create_date),''), '%Y-%m-%d');

-------------------------------Load for CUST_AZ12 -----------------------------------
LOAD DATA INFILE '/var/lib/mysql-files/CUST_AZ12.csv'
INTO TABLE bronze_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@CID,@GEN,@BDATE)
SET
CID = NULLIF(TRIM(@CID),''),
GEN = NULLIF(TRIM(@GEN),''),
BDATE = STR_TO_DATE(NULLIF(TRIM(@BDATE),''),'%Y-%m-%d');

-------------------------------load for sales_details-----------------------------------
LOAD DATA INFILE '/var/lib/mysql-files/sales_details.csv'
INTO TABLE bronze_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@sls_ord_num,
 @sls_prd_key,
 @sls_cust_id,
 @sls_order_dt,
 @sls_ship_dt,
 @sls_due_dt,
 @sls_sales,
 @sls_quantity,
 @sls_price)
SET
sls_ord_num = NULLIF(TRIM(@sls_ord_num),''),
sls_prd_key = NULLIF(TRIM(@sls_prd_key),''),
sls_cust_id = NULLIF(TRIM(@sls_cust_id),''),
sls_order_dt = CASE 
    WHEN TRIM(@sls_order_dt) REGEXP '^[0-9]{8}$'
    THEN STR_TO_DATE(@sls_order_dt,'%Y%m%d')
    ELSE NULL
END,
sls_ship_dt = CASE 
    WHEN TRIM(@sls_ship_dt) REGEXP '^[0-9]{8}$'
    THEN STR_TO_DATE(@sls_ship_dt,'%Y%m%d')
    ELSE NULL
END,
sls_due_dt = CASE 
    WHEN TRIM(@sls_due_dt) REGEXP '^[0-9]{8}$'
    THEN STR_TO_DATE(@sls_due_dt,'%Y%m%d')
    ELSE NULL
END,
sls_sales = NULLIF(TRIM(@sls_sales),''),
sls_quantity = NULLIF(TRIM(@sls_quantity),''),
sls_price = NULLIF(TRIM(@sls_price),'');

--------------------------load for LOC_A101------------------------------------------
LOAD DATA INFILE '/var/lib/mysql-files/LOC_A101.csv'
INTO TABLE bronze_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@CID,@CNTRY)
SET
CID = NULLIF(TRIM(@CID),''),
CNTRY = NULLIF(TRIM(@CNTRY),'');

-------------------------load for PX_CAT_G1V2---------------------------------------
LOAD DATA INFILE '/var/lib/mysql-files/PX_CAT_G1V2.csv'
INTO TABLE bronze_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@ID,@CAT,@SUBCAT,@MAINTENANCE)
SET
ID = NULLIF(TRIM(@ID),''),
CAT = NULLIF(TRIM(@CAT),''),
SUBCAT = NULLIF(TRIM(@SUBCAT),''),
MAINTENANCE = NULLIF(TRIM(@MAINTENANCE),'');

---------------------------prd_info----------------------------------------------------
LLOAD DATA INFILE '/var/lib/mysql-files/prd_info.csv'
INTO TABLE bronze_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(@prd_id,
 @prd_key,
 @prd_nm,
 @prd_cost,
 @prd_line,
 @prd_start_dt,
 @prd_end_dt)
SET
prd_id = NULLIF(TRIM(@prd_id),''),
prd_key = NULLIF(TRIM(@prd_key),''),
prd_nm = NULLIF(TRIM(@prd_nm),''),
prd_cost = NULLIF(TRIM(@prd_cost),''),
prd_line = NULLIF(TRIM(@prd_line),''),
prd_start_dt = STR_TO_DATE(NULLIF(NULLIF(TRIM(@prd_start_dt),'0'),''),'%Y%m%d'),
prd_end_dt = STR_TO_DATE(NULLIF(NULLIF(TRIM(@prd_end_dt),'0'),''),'%Y%m%d');
