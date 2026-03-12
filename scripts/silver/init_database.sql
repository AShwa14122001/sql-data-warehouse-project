create table silver_crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int ,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int
    );

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,

CASE 
    WHEN sls_order_dt = 0 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR),'%Y-%m-%d')
END AS sls_order_dt,

CASE 
    WHEN sls_ship_dt = 0 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR),'%Y-%m-%d')
END AS sls_ship_dt,

CASE 
    WHEN sls_due_dt = 0 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR),'%Y-%m-%d')
END AS sls_due_dt,

CASE 
    WHEN sls_sales IS NULL 
         OR sls_sales <= 0 
         OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

CASE 
    WHEN sls_price IS NULL OR sls_price <= 0   
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS corrected_price
FROM bronze_sales_details;


- (clean and load erp_cust_az12_table)

Insert into silver_erp_cust_az12(CID,BDATE,GEN)
SELECT 
CASE 
    WHEN CID LIKE 'NAS%' 
    THEN SUBSTRING(CID,4)
    ELSE CID
END AS CID,
CASE 
    WHEN BDATE > CURRENT_DATE 
    THEN NULL
    ELSE BDATE
END AS BDATE,
CASE
	WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') then 'Female'
    WHEN UPPER(TRIM(GEN)) IN ('M','MALE') then 'Male'
    ELSE 'N/A'
END AS GEN
FROM bronze_cust_az12
WHERE CID NOT IN (
    SELECT DISTINCT cst_key
    FROM silver_crm_cust_info
);


(Clean and load erp_loc_a101)

INSERT into silver_erp_loc_a101(CID, CNTRY)
select 
REPLACE(CID,'-','')  CID,
CASE WHEN TRIM(CNTRY) IN ('DE','Germany') THEN 'Germany'
     WHEN TRIM(CNTRY) IN ('US','USA') THEN 'USA'
     WHEN TRIM(CNTRY) = '' or CNTRY is NULL THEN 'n/a'
     ELSE TRIM(CNTRY)
END AS CNTRY
from bronze_loc_a101;
