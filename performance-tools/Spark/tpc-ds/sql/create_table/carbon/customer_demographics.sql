create table ${DB}.customer_demographics
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int                       
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
