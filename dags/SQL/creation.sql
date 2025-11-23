
CREATE OR REPLACE SCHEMA raw;


CREATE OR REPLACE TABLE ecommerce_table (
    InvoiceNo      VARCHAR,
    StockCode      VARCHAR,
    Description    VARCHAR,
    Quantity       NUMBER,
    InvoiceDate    TIMESTAMP,
    UnitPrice      FLOAT,
    CustomerID     VARCHAR,
    Country        VARCHAR
);


CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI'
  ENCODING = 'ISO8859-1';

  

CREATE OR REPLACE STAGE ecommerce_stage
URL = 's3://ecommerce-dataops/raw/data.csv'
CREDENTIALS = (AWS_KEY_ID='AKIA5XXVPK4ANOIS6O2K' AWS_SECRET_KEY='ZmPXXqd4N7Zs/wMYFxaJE5E8j8GkB8aCm+MVuzI6')
FILE_FORMAT = (format_name= raw.my_csv_format);




