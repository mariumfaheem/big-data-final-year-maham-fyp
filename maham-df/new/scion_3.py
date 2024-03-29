

import pyspark
from pyspark.sql.functions import rand
import datetime

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType,FloatType, DoubleType

from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import argparse
import datetime
from pyspark.sql import SparkSession
from datetime import datetime
spark = SparkSession.builder.getOrCreate()

"""## Global Variables"""

MoneyMule = 50.00

code_not_to_use = [1, 18, 111, 30000, 31813, 32190, 33205, 33206, 37505, 99999 ]

high_risk_categories = ['VH', 'HI', 'PE']

so_scr = ['SO', 'SCR']

"""## Criteria"""

A_Criteria = 499.99
B_Criteria = 499.99
C_Criteria = 4999.99
D_Criteria_Lower = 2499.99
D_Criteria_Upper = 4999.99
E_Criteria= 4999.99
F_Criteria = 499.99
G_Criteria = 2499.99

Y_Criteria = 500.00
Z_Criteria = 5999.99

Minor = 16
MuleLower = 17
MuleUpper = 25

parser = argparse.ArgumentParser(description='User defined arguments')

parser.add_argument('--codes_not_to_use', type=str, help='User defined codes_not_to_use')
parser.add_argument('--high_risk_categories', type=str, help='User defined high_risk_categories')
parser.add_argument('--so_scr', type=str, help='User defined so_scr')




parser.add_argument('--user_id', type=str, help='User defined user_id')
parser.add_argument('--A_Criteria', type=str, help='User defined A_Criteria')
parser.add_argument('--B_Criteria', type=str, help='User defined B_Criteria')
parser.add_argument('--C_Criteria', type=str, help='User defined C_Criteria')
parser.add_argument('--D_Criteria_Lower', type=str, help='User defined D_Criteria_Lower')
parser.add_argument('--D_Criteria_Upper', type=str, help='User defined D_Criteria_Upper')
parser.add_argument('--E_Criteria', type=str, help='User defined E_Criteria')
parser.add_argument('--F_Criteria', type=str, help='User defined F_Criteria')
parser.add_argument('--G_Criteria', type=str, help='User defined G_Criteria')
parser.add_argument('--Y_Criteria', type=str, help='User defined Y_Criteria')
parser.add_argument('--Z_Criteria', type=str, help='User defined Z_Criteria')
parser.add_argument('--MoneyMule', type=str, help='User defined MoneyMule')

parser.add_argument('--MuleHigher', type=str, help='User defined MuleHigher')
parser.add_argument('--MuleLower', type=str, help='User defined MuleLower')

args = parser.parse_args()

print(f"Received argument codes_not_to_use: {args.codes_not_to_use}")
print(f"Received argument high_risk_categories: {args.high_risk_categories}")
print(f"Received argument so_scr: {args.so_scr}")

print(f"Received argument A_Criteria: {args.A_Criteria}")
print(f"Received argument B_Criteria: {args.B_Criteria}")
print(f"Received argument C_Criteria: {args.C_Criteria}")
print(f"Received argument D_Criteria_Lower: {args.D_Criteria_Lower}")
print(f"Received argument D_Criteria_Upper: {args.D_Criteria_Upper}")
print(f"Received argument E_Criteria: {args.E_Criteria}")
print(f"Received argument F_Criteria: {args.F_Criteria}")
print(f"Received argument G_Criteria: {args.G_Criteria}")

print(f"Received argument Y_Criteria: {args.Y_Criteria}")
print(f"Received argument Z_Criteria: {args.Z_Criteria}")
print(f"Received argument MuleHigher: {args.MuleHigher}")
print(f"Received argument MuleLower: {args.MuleLower}")
print(f"Received argument MoneyMule: {args.MoneyMule}")
print(f"Received argument user_id: {args.user_id}")


codes_not_to_use =args.codes_not_to_use
high_risk_categories = args.high_risk_categories
so_scr = args.so_scr

Y_Criteria = args.Y_Criteria
Z_Criteria = args.Z_Criteria
MuleLower = args.MuleLower
MuleHigher = args.MuleHigher
MoneyMule = args.MoneyMule

A_Criteria = args.A_Criteria
B_Criteria = args.B_Criteria
C_Criteria = args.C_Criteria
D_Criteria_Lower = args.D_Criteria_Lower
D_Criteria_Upper = args.D_Criteria_Upper
E_Criteria = args.E_Criteria
F_Criteria = args.F_Criteria
G_Criteria = args.G_Criteria
user_id = args.user_id


"""## AWS S3 File Path"""

trans_path = "s3://alm-platform-bucket/source/demo/ScionTransactionList.csv"
member_path = "s3://alm-platform-bucket/source/demo/ScionMemberList.csv"

member_df = spark.read.option("header", True).csv(member_path)

trans_df = spark.read.option("header", True).csv(trans_path)

"""# Read Scion Transaction File"""

trans_df.show()

trans_df.printSchema()

"""## Data Pre-Processing"""

trans_df = trans_df.withColumnRenamed('Trans In Amount ', 'TransInAmount')
trans_df = trans_df.withColumnRenamed('Trans Out Amount', 'TransOutAmount')


pattern = r"\,"
trans_df = trans_df.withColumn(
    "TransInAmount",
    regexp_replace(trans_df["TransInAmount"], pattern, "")  # Replace comma with empty string
)

trans_df = trans_df.withColumn(
    "TransOutAmount",
    regexp_replace(trans_df["TransOutAmount"], pattern, "")  # Replace comma with empty string
)


"""# Calculate Columns from `Transaction File`"""

trans_df = trans_df.withColumn("TransactionTotal", col("TransInAmount") - col("TransOutAmount"))

trans_df = trans_df.withColumn(
    "TransactionPositiveTotal",
    when(col("TransactionTotal").isNull(), "").otherwise(abs(col("TransactionTotal")))
)

trans_df = trans_df.withColumn(
    "TransForMoneyMuleAndCombined",
    when(col("TransactionPositiveTotal") > MoneyMule, col("TransactionPositiveTotal")).otherwise(0)
)

trans_df = trans_df.withColumn(
    "Trans Incl",
    when(col("TransactionPositiveTotal") > 0, "Yes").otherwise("No")
)

trans_df = trans_df.withColumn(
    "Account_Not_In_Use",
    when(col("Account Number").isin(code_not_to_use), "No").otherwise("Yes")
)

trans_df = trans_df.withColumn(
    "SO_SCR",
    when(col("Transaction Type").isin(so_scr), col('Transaction Type')).otherwise("")
)


### No Logic start

# Define the window specification
windowSpec = Window.orderBy("Account Number")

# Add previous, current, and next values
df_with_prev_next = trans_df.withColumn("previous_value", lag("SO_SCR").over(windowSpec)) \
    .withColumn("next_value", lead("SO_SCR").over(windowSpec))


# No1 previous value = SCR and current value = s0 and next value = s0
df_with_prev_next =df_with_prev_next.withColumn(
    "No1",
    when((col("previous_value") == "SCR") & (col('SO_SCR') == "SO") & ( col('next_value') =="SO"), "Y").otherwise(" ")
)
df_with_prev_next.filter(col("Transaction No")==46996114).show()

#No2 previous n1 =’Y’ and  SO_SCR = s0. then Y else “”
df_with_prev_next = df_with_prev_next.withColumn(
    "No2",
    when(
        (lag("No1", 1).over(windowSpec) == "Y") & (col("SO_SCR") == "SO"),
        "Y"
    ).otherwise("")
)
df_with_prev_next.filter(col("Transaction No")==47012712).show()

#No 3 current No1 = Y and next No2 = Y and  current_total transaction = next_total trasacation then E
df_with_prev_next = df_with_prev_next.withColumn(
    "No3",
    when(
        (col("No1") == "Y") &
        (lead("No2", 1).over(windowSpec) == "Y") &
        (col("TransactionTotal") == lead("TransactionTotal", 1).over(windowSpec)),
        "E"
    ).otherwise("")
)

df_with_prev_next.filter(col("Transaction No")==47012711).show()


#No 4  - if previous no2 == E then E
df_with_prev_next = df_with_prev_next.withColumn(
    "No4",
    when(
        (lag("No3", 1).over(windowSpec) == "E"),
        "E"
    ).otherwise("")
)

#
df_with_prev_next.filter(col("Transaction No")==46996115).show()



### No Logic end



"""# Member File"""

member_df.show()

"""# Joining the table"""

member_trans_df = trans_df.join(member_df,df_with_prev_next['Account Number']==df_with_prev_next['Account Number'], how="inner")

member_trans_df.show()

member_trans_df.filter(member_trans_df['Risk Code'] == "HI").show()

"""# Calculate Columns from `Member_trans_df`"""

member_trans_df = member_trans_df.withColumn(
    "HighRisk",
    when(col("Risk Code").isin(high_risk_categories), "Yes").otherwise("No")
)

# Account_Not_In_Use ki jaGw code_in_use ayeGw
member_trans_df = member_trans_df.withColumn("GreaterThan",
                               when((col("Account_Not_In_Use")=='Yes') & (col("Trans Incl")=='Yes') & ((col("TransInAmount") >= D_Criteria_Lower) & (col("TransInAmount")<= D_Criteria_Upper)), "Yes")
                               .otherwise("No"))

"""# Display the `member_trans_df` DataFrame"""

member_trans_df.show()

"""### Verification of the Column Created"""

member_trans_df.filter(col('GreaterThan') == "Yes").show(5)

member_trans_df.filter(col('HighRisk') == "Yes").show(5)

# code no tot use = yes
# trans incl yes
# high risk yes
# trans postive total > A_Crtiera

"""# Creteria Creation"""

member_trans_df = member_trans_df.withColumn("A",
                                               when((col("Account_Not_In_Use")=="Yes") # Account_Not_In_Use ki jaga code_in_use ayeGw column
                                                    & (col("HighRisk")=="Yes")
                                                    & (col("TransactionPositiveTotal") > A_Criteria ), "A")
                                               .otherwise(None))

counts_of_A = member_trans_df.filter(member_trans_df['A'] == 'A').count()
print("Count of records where column 'A' equals 'A':", counts_of_A)
print("==========================================================")
#
# c_s3_bucket_path = "s3://scion-platform-bucket/source/demo/scion_criteria.csv"




s3_base_path = "s3://alm-platform-bucket/data_products/"
date_path = datetime.now().strftime('%Y/%m/%d/')
output_file_name = "scion_criteria.csv"
# Full path including the date
c_s3_bucket_path = s3_base_path +user_id +date_path + output_file_name
member_trans_df .write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)
print(f"Data saved to {c_s3_bucket_path}")