#quality check
# it has 4 parts i.e. null checker, uniqueness of PK, data type checker, Foreign Key Constraints.
# each has its separate function and I have called function when required.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Create a Spark session
spark = SparkSession.builder.appName("DataValidation").getOrCreate()

# Load the fact and dimension tables
fact_df = spark.read.option("delimiter", "|").csv(r"gs://test-gsynergy/fact.transactions.dlm.gz", header=True, inferSchema=True)
dim_df = spark.read.option("delimiter", "|").csv(r"gs://test-gsynergy/hier.clnd.dlm.gz", header=True, inferSchema=True)

#1. Null checker for table
def check_non_null(df, columns):
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        print(f"Non-null check for column {col_name}: {null_count} null values.")
        
# passing the keys to check for null values
check_non_null(fact_df, ['order_id', 'sku_id','pos_site_id','fscldt_id','price_substate_id']) #fact table
check_non_null(dim_df, ['fscldt_id']) # dimension table


# 2. Uniqueness of Primary Keys
def check_uniqueness(df, primary_key_column):
    duplicate_count = df.groupBy(primary_key_column).count().filter("count > 1").count()
    print(f"Uniqueness check for column {primary_key_column}: {duplicate_count} duplicate entries.")

check_uniqueness(fact_df, 'order_id')
check_uniqueness(dim_df, 'fscldt_id')


# 3. Data Types checker
def check_data_types(df, column_types):
    for col_name, expected_type in column_types.items():
        actual_type = dict(df.dtypes)[col_name]
        print(f"Data type check for column {col_name}: Expected {expected_type}, Found {actual_type}.")
        
expected_column_types_fact = {
    'order_id': 'bigint',
    'sku_id': 'string',
    'fscldt_id': 'int',
    'pos_site_id': 'string',
    'price_substate_id': 'string'
}
expected_column_types_dim = {
    'fscldt_id' :'int'
}

check_data_types(fact_df, expected_column_types_fact)
check_data_types(dim_df, expected_column_types_dim)


# 4. Foreign Key Constraints 
def check_foreign_key_constraint(fact_df, dim_df, fact_fk_col, dim_pk_col):
    fact_fk_values = fact_df.select(fact_fk_col).distinct().collect()
    dim_pk_values = dim_df.select(dim_pk_col).distinct().collect()
    
    fact_fk_set = set(row[0] for row in fact_fk_values)
    dim_pk_set = set(row[0] for row in dim_pk_values)
    
    invalid_fk_count = len(fact_fk_set - dim_pk_set)
    print(f"Foreign key constraint check: {invalid_fk_count} invalid foreign key values found.")
    
check_foreign_key_constraint(fact_df, dim_df, 'fscldt_id', 'fscldt_id')

spark.stop()
