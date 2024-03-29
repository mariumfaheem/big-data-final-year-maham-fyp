import pyspark
from pyspark.sql.functions import rand
import datetime

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType,FloatType, DoubleType


from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *
import argparse

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


"""## Testing CSV *Files*"""

# aml_path='/content/drive/MyDrive/RachelCSV/progess_aml_test.csv'
# member_path='/content/drive/MyDrive/RachelCSV/progess_member_test.csv'
# trans_path='/content/drive/MyDrive/RachelCSV/Progess_transaction_test.csv'

# aml_path='/content/drive/MyDrive/RachelCSV/AMLRiskComplianceExtract-20240103111526.csv'
# member_path='/content/drive/MyDrive/RachelCSV/Member_View_Progress_RAW_DATA_Dec 2023.csv'
# trans_path='/content/drive/MyDrive/RachelCSV/Transaction_Print_ODP_RAW_DATA_Dec_2023.csv'

"""# Define the pre-defined values"""

Online_DC =[
    'web repayment',
    'realex lodgement',
    'web lodgement',
    'realex repayment'
]



debit_card_list = [
    'debit card contactless payment',
    'debit card e-commerce payment',
    'debit card funds transfer',
    'debit card fx fee',
    'debit card payment',
    'debit card payment cashback',
    'debit card refund'
]


sub_office_list =["AGK", "CBS", 'CSU', 'EOC', 'GSO', 'MHS', 'NCS', 'SAS', 'SMS']
high_risk_jurisidctions_countries = [
    "Albania",
    "Afghanistan",
    "Barbados",
    "Burkina Faso",
    "Cameroon",
    "Cayman Islands",
    "Croatia",
    "Congo (DRC)",
    "Democratic People’s Republic of Korea",
    "Gibraltar",
    "Haiti",
    "Iran",
    "Jamaica",
    "Jordan",
    "Mali",
    "Mozambique",
    "Myanmar",
    "Nigeria",
    "Panama",
    "Philippine",
    "Senegal",
    "South Africa",
    "South Sudan",
    "Syrian",
    "Tanzania",
    "Trinidad and Tobago",
    "Turkey",
    "Uganda",
    "United Arab Emirates",
    "Vanuatu",
    "Vietnam",
    "Yemen"
]

residency_countries = [
    'argentina',
    'australia',
    'austria',
    'bangladesh',
    'belarus',
    'belgium',
    'brazil',
    'bulgaria',
    'cameroon',
    'canada',
    'congo (drc)',
    'costa rica',
    'croatia',
    'czech republic',
    'ecuador',
    'england',
    'estonia',
    'finland',
    'france',
    'gambia',
    'georgia',
    'germany',
    'ghana',
    'hong kong',
    'hungary',
    'india',
    'ireland',
    'italy',
    'jordan',
    'latvia',
    'lebanon',
    'lithuania',
    'malawi',
    'malaysia',
    'malta',
    'mexico',
    'the netherlands',
    'new zealand',
    'nigeria',
    'northern ireland',
    'norway',
    'pakistan',
    'philippines',
    'poland',
    'portugal',
    'romania',
    'scotland',
    'slovakia',
    'slovenia',
    'somalia',
    'south africa',
    'spain',
    'tanzania',
    'thailand',
    'tunisia',
    'turkey',
    'uganda',
    'ukraine',
    'united kingdom',
    'united states',
    'wales',
    'zimbabwe'
]


codes_not_to_use = [2148, 9999, 14000, 17194, 18000, 19999, 20000, 21000, 21466, 22252, 22343, 22702, 23772, 24254,50000,999998]

"""# Criteria"""

MuleHigher = 25
MuleLower = 17
MoneyMule = 100.00

A_Criteria = 99.99
B_Criteria = 99.99
C_Criteria = 9899.99
D_Criteria_Lower = 2499.99
D_Criteria_Upper = 9899.99
E_Criteria=4899.99
F_Criteria = 499.99
G_Criteria = 2499.99

J_Criteria  = 499.99
K_Criteria  = 499.99
L_Criteria = 2499.99

M_Criteria = 2499.99
O_Criteria = 99.99
P_Criteria = 99.99
Q_Criteria = 99.99

Y_Criteria = 499.99
Z_Criteria = 9949.99

parser = argparse.ArgumentParser(description='User defined arguments')
parser.add_argument('--codes_not_to_use', type=str, help='User defined codes_not_to_use')
parser.add_argument('--Online_DC', type=str, help='User defined Online_DC')
parser.add_argument('--debit_card_list', type=str, help='User defined debit_card_list')
parser.add_argument('--sub_office_list', type=str, help='sub_office_list')
parser.add_argument('--high_risk_jurisidctions_countries', type=str, help='User defined high_risk_jurisidctions_countries')
parser.add_argument('--residency_countries', type=str, help='User defined residency_countries')
parser.add_argument('--A_Criteria', type=str, help='User defined A_Criteria')
parser.add_argument('--B_Criteria', type=str, help='User defined B_Criteria')
parser.add_argument('--C_Criteria', type=str, help='User defined C_Criteria')
parser.add_argument('--D_Criteria_Lower', type=str, help='User defined D_Criteria_Lower')
parser.add_argument('--D_Criteria_Upper', type=str, help='User defined D_Criteria_Upper')
parser.add_argument('--E_Criteria', type=str, help='User defined E_Criteria')
parser.add_argument('--F_Criteria', type=str, help='User defined F_Criteria')
parser.add_argument('--G_Criteria', type=str, help='User defined G_Criteria')
parser.add_argument('--J_Criteria', type=str, help='User defined J_Criteria')
parser.add_argument('--K_Criteria', type=str, help='User defined K_Criteria')
parser.add_argument('--L_Criteria', type=str, help='User defined L_Criteria')
parser.add_argument('--M_Criteria', type=str, help='User defined M_Criteria')
parser.add_argument('--O_Criteria', type=str, help='User defined O_Criteria')
parser.add_argument('--P_Criteria', type=str, help='User defined P_Criteria')
parser.add_argument('--Q_Criteria', type=str, help='User defined Q_Criteria')
parser.add_argument('--Y_Criteria', type=str, help='User defined Y_Criteria')
parser.add_argument('--Z_Criteria', type=str, help='User defined Z_Criteria')
parser.add_argument('--MoneyMule', type=str, help='User defined MoneyMule')

parser.add_argument('--MuleHigher', type=str, help='User defined MuleHigher')
parser.add_argument('--MuleLower', type=str, help='User defined MuleLower')

args = parser.parse_args()

print(f"Received argument codes_not_to_use: {args.codes_not_to_use}")
print(f"Received argument A_Criteria: {args.A_Criteria}")
print(f"Received argument B_Criteria: {args.B_Criteria}")
print(f"Received argument C_Criteria: {args.C_Criteria}")
print(f"Received argument D_Criteria_Lower: {args.D_Criteria_Lower}")
print(f"Received argument D_Criteria_Upper: {args.D_Criteria_Upper}")
print(f"Received argument E_Criteria: {args.E_Criteria}")
print(f"Received argument F_Criteria: {args.F_Criteria}")
print(f"Received argument G_Criteria: {args.G_Criteria}")
print(f"Received argument J_Criteria: {args.J_Criteria}")
print(f"Received argument K_Criteria: {args.K_Criteria}")
print(f"Received argument L_Criteria: {args.L_Criteria}")
print(f"Received argument M_Criteria: {args.M_Criteria}")
print(f"Received argument O_Criteria: {args.O_Criteria}")
print(f"Received argument P_Criteria: {args.P_Criteria}")
print(f"Received argument Q_Criteria: {args.Q_Criteria}")

print(f"Received argument Y_Criteria: {args.Y_Criteria}")
print(f"Received argument Z_Criteria: {args.Z_Criteria}")
print(f"Received argument MuleHigher: {args.MuleHigher}")
print(f"Received argument MuleLower: {args.MuleLower}")


print(f"Received argument Online_DC: {args.Online_DC}")
print(f"Received argument debit_card_list: {args.debit_card_list}")
print(f"Received argument sub_office_list: {args.sub_office_list}")
print(f"Received argument high_risk_jurisidctions_countries: {args.high_risk_jurisidctions_countries}")
print(f"Received argument residency_countries: {args.residency_countries}")

codes_not_to_use =args.codes_not_to_use
A_Criteria = args.A_Criteria
B_Criteria = args.B_Criteria
C_Criteria = args.C_Criteria
D_Criteria_Lower = args.D_Criteria_Lower
D_Criteria_Upper = args.D_Criteria_Upper
E_Criteria = args.E_Criteria
F_Criteria = args.F_Criteria
G_Criteria = args.G_Criteria
J_Criteria = args.J_Criteria
K_Criteria = args.K_Criteria
L_Criteria = args.L_Criteria
M_Criteria = args.M_Criteria
O_Criteria = args.O_Criteria
P_Criteria = args.P_Criteria
Q_Criteria = args.Q_Criteria
Y_Criteria = args.Y_Criteria
Z_Criteria = args.Z_Criteria
MuleLower = args.MuleLower
MuleHigher = args.MuleHigher

Online_DC =args.Online_DC
debit_card_list =args.debit_card_list
sub_office_list =args.sub_office_list
high_risk_jurisidctions_countries =args.high_risk_jurisidctions_countries
residency_countries =args.residency_countries

aml_path = "s3://alm-platform-bucket/source/demo/AMLRiskCompliance.csv"
member_path = "s3://alm-platform-bucket/source/demo/Member_View.csv"
trans_path = "s3://alm-platform-bucket/source/demo/Transaction.csv"

member_df = spark.read.option("header", True).csv(member_path)

trans_df = spark.read.option("header", True).csv(trans_path)

aml_df = spark.read.option("header", True).csv(aml_path)

trans_df = trans_df.fillna(0)

"""# Read Progress Transaction file"""

trans_df.show()

trans_df.printSchema()

"""# Select the Columns will use in making `Master` File"""

trans_select_df = trans_df.select("No", "Teller", "Transaction Detail", "Transaction Number", "C/D", "Date & Time", "Wk", "Adjustment", "Withdrawal", "Loan Issue", "Lodgement", "Repayment", "Interest", "Total Payment")

trans_select_df.show()

"""# Data Pre-processing
#### Remove comma (,) from amount i.e (4,200.00)
"""

pattern = r"\,"
trans_select_df = trans_select_df.withColumn(
    "Total Payment",
    regexp_replace(trans_select_df["Total Payment"], pattern, "")  # Replace comma with empty string
)

trans_select_df = trans_select_df.withColumn(
    "Lodgement",
    regexp_replace(trans_select_df["Lodgement"], pattern, "")  # Replace comma with empty string
)

trans_select_df = trans_select_df.withColumn(
    "Adjustment",
    regexp_replace(trans_select_df["Adjustment"], pattern, "")  # Replace comma with empty string
)

trans_select_df = trans_select_df.withColumn(
    "Withdrawal",
    regexp_replace(trans_select_df["Withdrawal"], pattern, "")  # Replace comma with empty string
)

trans_select_df = trans_select_df.withColumn(
    "Loan Issue",
    regexp_replace(trans_select_df["Loan Issue"], pattern, "")  # Replace comma with empty string
)

trans_select_df = trans_select_df.withColumn(
    "Repayment",
    regexp_replace(trans_select_df["Repayment"], pattern, "")  # Replace comma with empty string
)

trans_df = trans_select_df \
    .withColumn("No", trans_select_df["No"].cast(IntegerType())) \
    .withColumn("Transaction Number", trans_select_df["Transaction Number"].cast(IntegerType())) \
    .withColumn("Teller", trans_select_df["Teller"].cast(StringType())) \
    .withColumn("Transaction Detail", trans_select_df["Transaction Detail"].cast(StringType())) \
    .withColumn("C/D", trans_select_df["C/D"].cast(StringType())) \
    .withColumn("Date & Time", trans_select_df["Date & Time"].cast(StringType())) \
    .withColumn("Wk", trans_select_df["Wk"].cast(StringType())) \
    .withColumn("Adjustment", trans_select_df["Adjustment"].cast(FloatType())) \
    .withColumn("Withdrawal", trans_select_df["Withdrawal"].cast(FloatType())) \
    .withColumn("Loan Issue", trans_select_df["Loan Issue"].cast(FloatType())) \
    .withColumn("Lodgement", trans_select_df["Lodgement"].cast(FloatType())) \
    .withColumn("Repayment", trans_select_df["Repayment"].cast(FloatType())) \
    .withColumn("Interest", trans_select_df["Interest"].cast(FloatType())) \
    .withColumn("Total Payment", trans_select_df["Total Payment"].cast(FloatType()))

# Show the DataFrame with the new schema
trans_df.show()

trans_df.printSchema()

trans_df.show()

trans_df.filter(trans_df['Total Payment'] == -700).show()

trans_df.filter((trans_df.Lodgement < D_Criteria_Lower) & (trans_df.Lodgement > D_Criteria_Upper)).count()

trans_df.filter(((col("Lodgement") >= D_Criteria_Lower) & (col("Lodgement") <= D_Criteria_Upper))).count()

"""# EDA - Transaction Progress"""

# trans_df = trans_df.withColumn(
#     "positive_total_trans_amount",
#     when(col("Total Payment") > 0, col("Total Payment")).otherwise(0)
# )

trans_df = trans_df.withColumn('positive_total_trans_amount',
                   when(col('Total Payment') > 0, col('Total Payment'))
                   .otherwise(-col('Total Payment')))

trans_df = trans_df.withColumn("Codes_Not_To_USE",
                               when(col("No").isin(codes_not_to_use), "NO")
                               .otherwise("YES"))



trans_df = trans_df.withColumn("Tran_Detail",
                               when(trans_df["Transaction Detail"] =="Web Repayment" , "Online")
                               .when(trans_df["Transaction Detail"] == "CT Lodgement", "EFT")
                               .when(trans_df["Transaction Detail"] == "CT Repayment", "EFT")
                               .when(trans_df["Transaction Detail"] == "Dispersal Credit", "EFT")
                               .when(trans_df["Transaction Detail"] == "Dispersal Debit", "EFT")
                               .when(trans_df["Transaction Detail"] == "Web Withdrawal", "Online")
                               .when(trans_df["Transaction Detail"] == "Lodgement", "Counter")
                               .when(trans_df["Transaction Detail"] == "Realex Lodgement", "Realex")
                               .when(trans_df["Transaction Detail"] == "Withdrawal", "Counter")
                               .when(trans_df["Transaction Detail"] == "Lean Repayment", "Counter")
                               .when(trans_df["Transaction Detail"] == "Dividend Point Adjustment", "Adjustment")
                               .when(trans_df["Transaction Detail"] == "Web Lodgement", "Online")
                               .when(trans_df["Transaction Detail"] == "DD Withdrawal", "Direct Debit")
                               .when(trans_df["Transaction Detail"] == "Direct Debit Lodgement", "Direct Debit")
                               .when(trans_df["Transaction Detail"] == "Direct Debit Repayment", "Direct Debit")
                               .when(trans_df["Transaction Detail"] == "Transaction Event", "Transfer")
                               .when(trans_df["Transaction Detail"] == "Loan Issue", "Counter")
                               .when(trans_df["Transaction Detail"] == "Reversal", "Reversal")
                               .when(trans_df["Transaction Detail"] == "Web Transfer", "Online")
                               .when(trans_df["Transaction Detail"] == "Intra Transfer", "Transfer")
                               .when(trans_df["Transaction Detail"] == "Transfer", "Transfer")
                               .when(trans_df["Transaction Detail"] == "Standing Order Repayment", "Standing Order")
                               .when(trans_df["Transaction Detail"] == "Standing Order Lodgement", "Standing Order")
                               .when(trans_df["Transaction Detail"] == "Realex Repayment", "Realex")
                               .when(trans_df["Transaction Detail"] == "Interest Due Adjustmentt", "Adjustment")
                               .otherwise("Other"))



trans_df = trans_df.withColumn("Online_DC_Pmt",
                               when(lower(col("Transaction Detail")).isin(Online_DC), "YES")
                               .otherwise("NO"))

trans_df = trans_df.withColumn("Debit Card",
                               when(lower(col("Transaction Detail")).isin(debit_card_list), "YES")
                               .otherwise("NO"))


trans_df = trans_df.withColumn("Ldg",
                               when((col("Codes_Not_To_USE")=='YES') & ((col("Lodgement") >= D_Criteria_Lower) & (col("Lodgement") <= D_Criteria_Upper)), "YES")
                               .otherwise("NO"))

trans_df = trans_df.withColumn("sub_office",
                               when(upper(col("Teller")).isin(sub_office_list), "YES")
                               .otherwise("NO"))

trans_df.filter((col("Codes_Not_To_USE")=='YES') & ((col("Lodgement") >= D_Criteria_Lower) & (col("Lodgement") <= D_Criteria_Upper))).count()

trans_df.show()

"""# Read AML Progress File"""

aml_df.show()

aml_df.printSchema()

select_aml_df = aml_df.select("MemberNumber" , "IntendedPurposeOfBusinessRelationship", "DateJoined", "AccountType", "LengthOfBusinessRelationship", "CountryOfResidence_BasedOnMemberActiveAddress")

aml_schema = StructType([
    StructField("MemberNumber", IntegerType(), True),
    StructField("IntendedPurposeOfBusinessRelationship", StringType(), True),
    StructField("DateJoined", StringType(), True),
    StructField("AccountType", StringType(), True),
    StructField("LengthOfBusinessRelationship", StringType(), True),
    StructField("CountryOfResidence_BasedOnMemberActiveAddress", StringType(), True)
])

# Apply the schema to select_aml_df
select_aml_df = select_aml_df \
    .withColumn("MemberNumber", select_aml_df["MemberNumber"].cast(IntegerType())) \
    .withColumn("IntendedPurposeOfBusinessRelationship", select_aml_df["IntendedPurposeOfBusinessRelationship"].cast(StringType())) \
    .withColumn("DateJoined", select_aml_df["DateJoined"].cast(StringType())) \
    .withColumn("AccountType", select_aml_df["AccountType"].cast(StringType())) \
    .withColumn("LengthOfBusinessRelationship", select_aml_df["LengthOfBusinessRelationship"].cast(StringType())) \
    .withColumn("CountryOfResidence_BasedOnMemberActiveAddress", select_aml_df["CountryOfResidence_BasedOnMemberActiveAddress"].cast(StringType()))

# Show the DataFrame with the new schema
select_aml_df.show()

"""# Change Columns Names
1. IntendedPurposeOfBusinessRelationship -> PersonalBusiness
2. LengthOfBusinessRelationship -> NewMember
3. CountryOfResidence_BasedOnMemberActiveAddress --> Country
"""

select_aml_df = select_aml_df.withColumnRenamed('IntendedPurposeOfBusinessRelationship', 'PersonalBusiness')
select_aml_df = select_aml_df.withColumnRenamed('LengthOfBusinessRelationship', 'NewMember')
select_aml_df = select_aml_df.withColumnRenamed('CountryOfResidence_BasedOnMemberActiveAddress', 'Country')

select_aml_df.show(10)

"""# Read Member View File"""

member_df.show()

member_df.printSchema()

select_member_df = member_df.select(
    member_df['Member_ID'].cast(IntegerType()),
    member_df["MemberNumber"].cast(IntegerType()).alias("MemberNumber"),
    member_df["Age"].cast(IntegerType()).alias("Age"),
    "Nationality",
    "RiskLevel",
    "RiskLevelDescription",
    "BusinessRelationship" if "BusinessRelationship" in member_df.columns else member_df["Business"].alias("BusinessRelationship")
)

# Define the schema with all columns as IntegerType except Age, which is StringType
member_schema = StructType([
    StructField("MemberNumber", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Nationality", StringType(), True),
    StructField("RiskLevel", StringType(), True),
    StructField("RiskLevelDescription", StringType(), True),
    StructField("BusinessRelationship", StringType(), True)
])

# Apply the schema to select_member_df
select_member_df = select_member_df \
    .withColumn("MemberNumber", select_member_df["MemberNumber"].cast(IntegerType()))

# Show the DataFrame with the new schema
select_member_df.show()

"""# Transformation

### Create Column Named `Business`

`Rules to check`
if `BusinessRelationship` == `Business` then `Yes` else `No`
"""

select_member_df = select_member_df.withColumn("Business",
    when(select_member_df["BusinessRelationship"] == "Business", "Yes").otherwise("No")
)

condition = (col("Age") > MuleLower) & (col("Age") < MuleHigher)
select_member_df = select_member_df.withColumn("AgeForMule", when(condition, "YES").otherwise("NO"))

"""### Check that the column added and working properly"""

select_member_df.show()

"""# Joining the DataFrames"""

member_trans_df = trans_df.join(select_member_df,trans_df.No==select_member_df.MemberNumber, how="inner")
member_trans_aml = member_trans_df.join(select_aml_df,member_trans_df.MemberNumber == select_aml_df.MemberNumber, how = "left")

member_trans_aml.show(5)

"""# Display the DataFrame after Joining the three DataFrame"""

member_trans_aml.show(20)

member_trans_aml.printSchema()

member_trans_aml.printSchema()

member_trans_aml.filter((member_trans_aml['Debit Card'] == "YES") & (member_trans_aml['positive_total_trans_amount'] > J_Criteria) & (member_trans_aml['Codes_Not_To_USE'] == "YES")).count()

"""## Extract Only Date from `Date & Time` Column"""

member_trans_aml = member_trans_aml.withColumn("Date", substring("Date & Time", 1, 8))
member_trans_aml.show(2)

"""## Create `Sum_Lodgement` Column"""

from pyspark.sql import Window

window_spec = Window.partitionBy("No")

member_trans_aml = member_trans_aml.withColumn(
    "Sum_Lodgement",
    when(col("Codes_Not_To_USE") == "NO", 0)
    .otherwise(format_number(sum(col("Lodgement")).over(window_spec), 2))
)

"""## Data Preprocessing on `Sum_Lodgement`"""

pattern = r"\,"
member_trans_aml = member_trans_aml.withColumn(
    "Sum_Lodgement",
    regexp_replace(member_trans_aml["Sum_Lodgement"], pattern, "")  # Replace comma with empty string
)

member_trans_aml = member_trans_aml.withColumn("Mule",
    when(
        (col("Sum_Lodgement") > Y_Criteria) &
        (col("AgeForMule") == "YES"),
        "Y"
    ).otherwise("")
)

member_trans_aml = member_trans_aml.withColumn("SumGreaterThanZ",
    when(
        (col("Sum_Lodgement") > Z_Criteria),
        "Z"
    ).otherwise("")
)

member_trans_aml = member_trans_aml.withColumn("MoneyMuleAcc",
    when(
        (col("Mule") == "Y"),
        "Y"
    ).otherwise("")
)

member_trans_aml.filter(member_trans_aml['Ldg'] == "YES").count()

member_trans_aml.filter(member_trans_aml['No'] == 14369).show()

member_trans_aml.show()

member_trans_aml.filter((col("Codes_Not_To_USE")=='YES') & ((col("Ldg") == "YES"))).count()



member_trans_aml.filter((col("positive_total_trans_amount")>J_Criteria ) & (col("Debit Card")=="Yes")).show()

"""# Apply the Criteria"""

member_trans_aml = member_trans_aml.withColumn("A",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount") >= A_Criteria )
                                                    & (col("RiskLevel")=="H"), "A")
                                               .otherwise(""))


member_trans_aml = member_trans_aml.withColumn("B",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount") >= B_Criteria )
                                                    & ((col("Nationality")).isin(high_risk_jurisidctions_countries)), "B")
                                               .otherwise(""))


member_trans_aml = member_trans_aml.withColumn("C",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement") > C_Criteria ), "C")
                                               .otherwise(""))

member_trans_aml = member_trans_aml.withColumn("D",
                                               when((col("Ldg")=="YES"), "D")
                                               .otherwise(""))

# TRANS_DETAIL is not an amount, it's a details of TRANSACTION --> In String
member_trans_aml = member_trans_aml.withColumn("E",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement") > E_Criteria )
                                                    & (col("Tran_Detail")=="EFT"), "E")
                                               .otherwise(""))

member_trans_aml = member_trans_aml.withColumn("F",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")>F_Criteria )
                                                    & (col("AccountType")=="Minor"), "F")
                                               .otherwise(""))


member_trans_aml = member_trans_aml.withColumn("G",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")> G_Criteria )
                                                    & (col("sub_office")=="YES"), "G")
                                               .otherwise(""))


member_trans_aml = member_trans_aml.withColumn("J",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>J_Criteria )
                                                    & (col("Debit Card")=="YES"), "J")
                                               .otherwise(""))

member_trans_aml = member_trans_aml.withColumn("K",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Total Payment")>K_Criteria )
                                                    & (col("Online_DC_Pmt")=="YES"), "K")
                                               .otherwise(""))



member_trans_aml = member_trans_aml.withColumn("L",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")>L_Criteria)
                                                    & (col("NewMember")=="0 - 3 Months"), "L")
                                               .otherwise(""))


"""
Business Relationship == Business
it's similar to IntendedPurposeOfBusinessRelationship OR business column created with IntendedPurposeOfBusinessRelationship == YES???
"""


member_trans_aml = member_trans_aml.withColumn("M",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")> M_Criteria )
                                                    & (col("BusinessRelationship")=="Business"), "M")
                                               .otherwise(""))



member_trans_aml = member_trans_aml.withColumn("O",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Teller") == "ATM")
                                                    & (col("positive_total_trans_amount") > O_Criteria), "O")
                                               .otherwise(""))

member_trans_aml = member_trans_aml.withColumn("P",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>P_Criteria)
                                                    & (lower(col("Nationality")).isin("russia","ukraine")) , "P")
                                               .otherwise(""))



member_trans_aml = member_trans_aml.withColumn("Q",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")> Q_Criteria)
                                                    & (lower(col("Nationality")).isin(high_risk_jurisidctions_countries)) , "Q")
                                               .otherwise(""))


member_trans_aml = member_trans_aml.withColumn("Y",
    when(
        (col("positive_total_trans_amount") > MoneyMule) &
        (col("MoneyMuleAcc") == "Y"),
        "Y"
    ).otherwise("")
)

member_trans_aml = member_trans_aml.withColumn("Z",
    when(
        (col("positive_total_trans_amount") > MoneyMule) &
        (col("SumGreaterThanZ") == "Z"),
        "Z"
    ).otherwise("")
)

"""### Filter that show values are correct"""

member_trans_aml.filter(member_trans_aml['Transaction Number'] == 19302967).show()

final_criteria = member_trans_aml.select("A","B", "C", "D", "E", "F", "G", "J", "K", "L", "M", "O", "Y", "Z")

df =  member_trans_aml.withColumn('Criteria', concat(member_trans_aml.A, member_trans_aml.B, member_trans_aml.C, member_trans_aml.D, member_trans_aml.E, member_trans_aml.F, member_trans_aml.G, member_trans_aml.J, member_trans_aml.K, member_trans_aml.L,member_trans_aml.M,member_trans_aml.O))
df.show()

df_filtered_output = df.filter(df['Criteria'] != "")

# Show the filtered DataFrame
df_filtered_output.show()

"""# Testing the final Criteria

##### Counts Records of criteria
"""

counts_of_A = member_trans_aml.filter(member_trans_aml['A'] == 'A').count()
print("Count of records where column 'A' equals 'A':", counts_of_A)
print("==========================================================")

counts_of_B = member_trans_aml.filter(member_trans_aml['B'] == 'B').count()
print("Count of records where column 'B' equals 'B':", counts_of_B)
print("==========================================================")


counts_of_C = member_trans_aml.filter(member_trans_aml['C'] == 'C').count()
print("Count of records where column 'C' equals 'C':", counts_of_C)
print("==========================================================")


counts_of_D = member_trans_aml.filter(member_trans_aml['D'] == 'D').count()
print("Count of records where column 'D' equals 'D':", counts_of_D)
print("==========================================================")


counts_of_E = member_trans_aml.filter(member_trans_aml['E'] == 'E').count()
print("Count of records where column 'E' equals 'E':", counts_of_E)
print("==========================================================")


counts_of_F = member_trans_aml.filter(member_trans_aml['F'] == 'F').count()
print("Count of records where column 'F' equals 'F':", counts_of_F)
print("==========================================================")


counts_of_G = member_trans_aml.filter(member_trans_aml['G'] == 'G').count()
print("Count of records where column 'G' equals 'G':", counts_of_G)
print("==========================================================")


counts_of_J = member_trans_aml.filter(member_trans_aml['J'] == 'J').count()
print("Count of records where column 'J' equals 'J':", counts_of_J)
print("==========================================================")


counts_of_K = member_trans_aml.filter(member_trans_aml['K'] == 'K').count()
print("Count of records where column 'K' equals 'K':", counts_of_K)
print("==========================================================")


counts_of_L = member_trans_aml.filter(member_trans_aml['L'] == 'L').count()
print("Count of records where column 'L' equals 'L':", counts_of_L)
print("==========================================================")

counts_of_M = member_trans_aml.filter(member_trans_aml['M'] == 'M').count()
print("Count of records where column 'M' equals 'M':", counts_of_M)
print("==========================================================")


counts_of_O = member_trans_aml.filter(member_trans_aml['O'] == 'O').count()
print("Count of records where column 'O' equals 'O':", counts_of_O)
print("==========================================================")


counts_of_P = member_trans_aml.filter(member_trans_aml['P'] == 'P').count()
print("Count of records where column 'P' equals 'P':", counts_of_P)
print("==========================================================")


counts_of_Q = member_trans_aml.filter(member_trans_aml['Q'] == 'Q').count()
print("Count of records where column 'Q' equals 'Q':", counts_of_Q)
print("==========================================================")

counts_of_Y = member_trans_aml.filter(member_trans_aml['Y'] == 'Y').count()
print("Count of records where column 'Y' equals 'Y':", counts_of_Y)
print("==========================================================")

counts_of_Z = member_trans_aml.filter(member_trans_aml['Z'] == 'Z').count()
print("Count of records where column 'Z' equals 'Z':", counts_of_Z)
print("==========================================================")

not_null_criteria = member_trans_aml.select("A","B", "C", "E", "F", "G", "J", "K", "L", "M", "O","P","Q", "Y", "Z")

final_criteria = member_trans_aml.select("No","Teller","Transaction Detail","Transaction Number","C/D", "Date",
                                         "Wk",
                                         "Adjustment",
                                         "Withdrawal",
                                         "Loan Issue",
                                         "Lodgement",
                                         "Repayment",
                                         "Interest",
                                         "Total Payment",
                                         "positive_total_trans_amount",
                                         "Codes_Not_To_USE",
                                         "Tran_Detail",
                                         "Online_DC_Pmt",
                                         "Business",
                                         "Ldg",
                                         "Age",
                                         "Nationality",
                                         "RiskLevel",
                                         "BusinessRelationship",
                                         "AccountType",
                                         "NewMember",
                                         "Debit Card","sub_office", "PersonalBusiness","Country", "MoneyMuleAcc", "SumGreaterThanZ",
                                         "A","B", "C", "D", "E", "F", "G", "J", "K", "L", "M", "O","P","Q", "Y", "Z")

"""## Output File - ABC  Credit Union"""

output_df = df_filtered_output.select(
    col("No").alias("MemberId"),
    col("Date"),
    col("Transaction Number"),
    col("Total Payment").alias("Total_PMT"),
    col("Criteria")
)

output_df.show()

output_df_count = output_df.count()
print(output_df_count)

rows = final_criteria.count()
print(rows)

"""## Master File Store"""

# c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/criteria.csv"
# condition = coalesce(*[col(c) for c in not_null_criteria.columns]) != lit("")
# df_filtered = final_criteria.filter(condition)
# df_filtered.coalesce(1).write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)

# c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/criteria.csv"
# final_criteria.coalesce(1).write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)

c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/criteria.csv"
final_criteria.write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)

"""# Output File Store"""

# c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/output.csv"
# output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)


c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/output.csv"
output_df.write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)