import pyspark
from pyspark.sql.functions import rand
import datetime
import argparse
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType,FloatType,DoubleType


from pyspark import SparkContext, HiveContext
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


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

Online_DC =args.Online_DC
debit_card_list =args.debit_card_list
sub_office_list =args.sub_office_list
high_risk_jurisidctions_countries =args.high_risk_jurisidctions_countries
residency_countries =args.residency_countries

aml_path = "s3://alm-platform-bucket/source/demo/AMLRiskCompliance.csv"
member_path = "s3://alm-platform-bucket/source/demo/Member_View.csv"
trans_path = "s3://alm-platform-bucket/source/demo/Transaction.csv"



trans_schema = StructType([
    StructField("No", IntegerType(), True),
    StructField("Teller", StringType(), True),
    StructField("Transaction Detail", StringType(), True),
    StructField("Transaction Number", IntegerType(), True),
    StructField("C/D", StringType(), True),
    StructField("Date & Time", StringType(), True),
    StructField("Wk", StringType(), True),
    StructField("Adjustment", StringType(), True),
    StructField("Withdrawal", StringType(), True),
    StructField("Loan Issue", StringType(), True),
    StructField("Lodgement", StringType(), True),
    StructField("Repayment", IntegerType(), True),
    StructField("Interest", IntegerType(), True),
    StructField("Total Payment", StringType(), True), ###

])
aml_schema = StructType([
    StructField("MemberNumber", IntegerType(), True),
    StructField("DateJoined", StringType(), True),
    StructField("PoliticallyExposedPerson", StringType(), True),
    StructField("SecondACHolderPoliticallyExposedPerson", IntegerType(), True),
    StructField("AccountStatus", StringType(), True),
    StructField("AccountType", StringType(), True),
    StructField("IntendedPurposeOfBusinessRelationship", StringType(), True),
    StructField("AssetsHeldInAccount", StringType(), True),
    StructField("TransactionVolumesLastQuarter", StringType(), True),
    StructField("LengthOfBusinessRelationship", StringType(), True),
    StructField("QualityOfCDDOnFile", StringType(), True),
    StructField("CountryOfResidence_BasedOnMemberActiveAddress", StringType(), True),
    StructField("SecondACHolderCountryOfResidence", StringType(), True),
    StructField("FundsChannelIn", StringType(), True),
    StructField("FundsChannelOut", StringType(), True),
    StructField("DestinationOfFunds", StringType(), True),
    StructField("SourceOfWealth", StringType(), True),
    StructField("SecondACHolderSourceOfWealth", StringType(), True),
    StructField("SourceOfFunds", StringType(), True),
    StructField("SecondACHolderSourceOfFunds", StringType(), True),
    StructField("ExpextedMonthlyLodgement", StringType(), True),
    StructField("SecondACHolderExpextedMonthlyLodgement", StringType(), True),
    StructField("EmploymentStatus", StringType(), True),
    StructField("SecondACHolderEmploymentStatus", StringType(), True),

    StructField("Occupation", StringType(), True),
    StructField("SecondACHolderOccupation", StringType(), True),
    StructField("BeneficialOwner", StringType(), True),
    StructField("PrimaryServiceUsed", StringType(), True),

    StructField("AccountActivity", StringType(), True),
    StructField("OtherRiskFactors", StringType(), True),

])
member_schema = StructType([
    StructField("Member_ID", IntegerType(), True),
    StructField("MemberNumber", IntegerType(), True),
    StructField("MemberNumberSort", IntegerType(), True),
    StructField("StaffNumber", IntegerType(), True),
    StructField("IndividualNumber", IntegerType(), True),
    StructField("Title", StringType(), True),
    StructField("DateofBirth", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("CountryOfBirth", StringType(), True),
    StructField("SecondACHolderCountryOfBirth", StringType(), True),
    StructField("IsSoleTrader", StringType(), True),
    StructField("NonPersonalAccount", StringType(), True),
    StructField("IsClub", StringType(), True),
    StructField("IsBusiness", StringType(), True),
    StructField("NonPersonalAccountType", StringType(), True),
    StructField("JointAccount", StringType(), True),
    StructField("Column2", StringType(), True),
    StructField("Column3", StringType(), True),
    StructField("Column4", StringType(), True),
    StructField("SecondACHolderDOB", StringType(), True),
    StructField("SecondACHolderAge", StringType(), True),
    StructField("Column5", StringType(), True),
    StructField("BadDebt", StringType(), True),
    StructField("Dormant", StringType(), True),
    StructField("DateJoined", StringType(), True),
    StructField("AgeAtJoining", StringType(), True),
    StructField("Closed", StringType(), True),
    StructField("Relationship", StringType(), True),
    StructField("Deceased", StringType(), True),
    StructField("DateofDeath", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("SecondACHolderGender", StringType(), True),
    StructField("Mabs", StringType(), True),
    StructField("NI_Number", StringType(), True),
    StructField("MembershipType", StringType(), True),
    StructField("CreditUnionRole", StringType(), True),
    StructField("Officer", StringType(), True),
    StructField("MaritalStatus", StringType(), True),
    StructField("MaritalStatus_SecondMember", StringType(), True),
    StructField("SpouseMember_ID", StringType(), True),
    StructField("SpouseMemberNumber", StringType(), True),
    StructField("Column6", StringType(), True),
    StructField("Column7", StringType(), True),
    StructField("Column8", StringType(), True),
    StructField("Column9", StringType(), True),
    StructField("Column10", StringType(), True),
    StructField("Column11", StringType(), True),
    StructField("Column12", StringType(), True),
    StructField("AccommodationType", StringType(), True),
    StructField("AccommodationType_SecondMember", StringType(), True),
    StructField("MemberAnalysis", StringType(), True),
    StructField("MemberStatus", StringType(), True),
    StructField("GuardianMemberNumber", StringType(), True),
    StructField("Column13", StringType(), True),
    StructField("Column14", StringType(), True),
    StructField("Column15", StringType(), True),
    StructField("Column16", StringType(), True),
    StructField("Column17", StringType(), True),
    StructField("GuardianDateofBirth", StringType(), True),
    StructField("ExemptFromDIRT", StringType(), True),
    StructField("SupervisorLetterSent", StringType(), True),
    StructField("OptIntoDraw", StringType(), True),
    StructField("OptIntoDBI", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
    StructField("ActiveEmployer", StringType(), True),
    StructField("ActiveEmployer_SecondMember", StringType(), True),
    StructField("Occupation", StringType(), True),
    StructField("Occupation_SecondMember", StringType(), True),
    StructField("EmploymentStatus", StringType(), True),
    StructField("EmploymentStatus_SecondMember", StringType(), True),
    StructField("EmployerType", StringType(), True),
    StructField("EmployerType_SecondMember", StringType(), True),
    StructField("EmploymentTerm", StringType(), True),
    StructField("EmploymentTerm_SecondMember", StringType(), True),
    StructField("EconomicSectorOfEmployment", StringType(), True),
    StructField("EconomicSectorOfEmployment_SecondMember", StringType(), True),
    StructField("LevelOfEducation", StringType(), True),
    StructField("LevelOfEducation_SecondMember", StringType(), True),
    StructField("NoCorrespondence", StringType(), True),
    StructField("MemberArea", StringType(), True),
    StructField("CommonBond", StringType(), True),
    StructField("Column18", StringType(), True),
    StructField("Column19", StringType(), True),
    StructField("Column20", StringType(), True),
    StructField("Column21", StringType(), True),
    StructField("Column22", StringType(), True),
    StructField("Column23", StringType(), True),
    StructField("Column24", StringType(), True),
    StructField("Column25", StringType(), True),
    StructField("Column26", StringType(), True),
    StructField("Column27", StringType(), True),
    StructField("UserData1", StringType(), True),
    StructField("UserData2", StringType(), True),
    StructField("UserData3", StringType(), True),
    StructField("UserData4", StringType(), True),
    StructField("UserData5", StringType(), True),
    StructField("NCUConsentStatus", StringType(), True),
    StructField("Column28", StringType(), True),
    StructField("Column29", StringType(), True),
    StructField("Column30", StringType(), True),
    StructField("Column31", StringType(), True),
    StructField("Column32", StringType(), True),
    StructField("TotalSavings", StringType(), True),
    StructField("TotalLoans", StringType(), True),
    StructField("NumberofLoansTaken", StringType(), True),
    StructField("TotalValueofLoansTaken", StringType(), True),
    StructField("SavingsAttachedasGuarantor", StringType(), True),
    StructField("SavingsAttachedasMember", StringType(), True),
    StructField("NumNomineesOnFile", StringType(), True),
    StructField("Column33", StringType(), True),
    StructField("Nationality", StringType(), True),
    StructField("SecondACHolderNationality", StringType(), True),
    StructField("CountryofResidence", StringType(), True),
    StructField("SecondACHolderCountryOfResidence", StringType(), True),
    StructField("Column34", StringType(), True),
    StructField("TaxResidence", StringType(), True),
    StructField("DateLastWrittenOff", StringType(), True),
    StructField("DateLastMadeDormant", StringType(), True),
    StructField("DateLastTransactionAffectingDormancy", StringType(), True),
    StructField("RiskLevel", StringType(), True),
    StructField("RiskLevelDescription", StringType(), True),
    StructField("Business", StringType(), True),
    StructField("CreditStatus", StringType(), True),
    StructField("MarketingConsentsExpiryDate", StringType(), True),
    StructField("MarketingUpdateDate", StringType(), True),
    StructField("EmailMarketingPermitted", StringType(), True),
    StructField("TextMarketingPermitted", StringType(), True),
    StructField("PhoneMarketingPermitted", StringType(), True),
    StructField("PostMarketingPermitted", StringType(), True),
    StructField("textPersonalPermitted", StringType(), True),
    StructField("textPersonalPermittedDate", StringType(), True),
    StructField("PreferElectronicStatement", StringType(), True),
    StructField("PreferElectronicStatementDate", StringType(), True),
    StructField("PreferElectronicAGMBooklet", StringType(), True),
    StructField("PreferElectronicAGMBookletDate", StringType(), True),
    StructField("DetailsOnWeb", StringType(), True),
    StructField("BranchName", StringType(), True),
    StructField("DGSAcknowledged", StringType(), True),
    StructField("ExpectedMonthlyLodgement", StringType(), True),
    StructField("ExpectedMonthlyLodgement_SecondMember", StringType(), True),
    StructField("NumberOfDependants", StringType(), True),
    StructField("Column1", StringType(), True),
])



member_df = spark.read.option("header", True).schema(member_schema).csv(member_path)
trans_df = spark.read.option("header", True).schema(trans_schema).csv(trans_path)
aml_df = spark.read.option("header", True).schema(aml_schema).csv(aml_path)
trans_df = trans_df.fillna(0)

trans_df = trans_df.withColumn("Total Payment", regexp_replace("Total Payment", ",", "").cast(DoubleType()))
trans_df = trans_df.withColumn("Lodgement", regexp_replace("Total Payment", ",", "").cast(DoubleType()))


#EDA

trans_df = trans_df.withColumn(
    "positive_total_trans_amount",
    when(col("Total Payment") > 0, col("Total Payment")).otherwise(0).cast(FloatType())
)

trans_df = trans_df.withColumn("Codes_Not_To_USE",
                               when(col("No").isin(codes_not_to_use),
                                    when(col("No")>0, "NO").otherwise(" "))
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
                               when((col("Codes_Not_To_USE")=='YES') & ((col("Lodgement") >= D_Criteria_Lower) & (col("Lodgement")<= D_Criteria_Upper)), "YES")
                               .otherwise("NO"))



trans_df = trans_df.withColumn("sub_office",
                               when(upper(col("Teller")).isin(sub_office_list), "YES")
                               .otherwise("NO"))



member_df = member_df.withColumnRenamed('Business', 'BusinessRelationship')

member_trans_df = trans_df.join(member_df,trans_df.No==member_df.MemberNumber, how="left") # Before inner I've change it to 'LEFT'
member_trans_aml = member_trans_df.join(aml_df,member_trans_df.MemberNumber == aml_df.MemberNumber, how = "left")




member_trans_aml = member_trans_aml.withColumn("Business",
                                               when(col("IntendedPurposeOfBusinessRelationship")=="Business Savings", "YES")
                                               .otherwise("NO"))
member_trans_aml = member_trans_aml['No', 'Teller', 'Transaction Detail', 'Transaction Number', 'C/D', 'Date & Time', 'Wk', 'Adjustment', 'Withdrawal', 'Loan Issue', 'Lodgement', 'Repayment', 'Interest' , 'Total Payment', 'positive_total_trans_amount' , 'Codes_Not_To_USE', 'Tran_Detail', 'Online_DC_Pmt', 'Debit Card', 'Ldg','sub_office', 'Age', 'Nationality', 'RiskLevel', 'RiskLevelDescription', 'Business', 'BusinessRelationship',  'IntendedPurposeOfBusinessRelationship', 'AccountType', 'LengthOfBusinessRelationship']
member_trans_aml = member_trans_aml.withColumnRenamed('IntendedPurposeOfBusinessRelationship', 'PersonalBusiness')
member_trans_aml = member_trans_aml.withColumnRenamed('LengthOfBusinessRelationship', 'NewMember')





member_trans_aml = member_trans_aml.withColumn("A",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>= 12.00 )
                                                    & (col("RiskLevel")=="H"), "A")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("B",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount") >= B_Criteria )
                                                    & (lower(col("Nationality")).isin(high_risk_jurisidctions_countries)), "B")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("C",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement") > C_Criteria ), "C")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("E",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")> E_Criteria )
                                                    & (col("Tran_Detail")=="EFT"), "E")
                                               .otherwise(None))


member_trans_aml = member_trans_aml.withColumn("F",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")>F_Criteria )
                                                    & (col("AccountType")=="Minor"), "F")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("G",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")> G_Criteria )
                                                    & (col("sub_office")=="YES"), "G")
                                               .otherwise(None))


member_trans_aml = member_trans_aml.withColumn("J",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>J_Criteria )
                                                    & (col("Debit Card")=="Yes"), "J")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("K",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Total Payment")>K_Criteria )
                                                    & (col("Online_DC_Pmt")=="YES"), "K")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("L",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")>L_Criteria)
                                                    & (col("NewMember")=="0 - 3 Months"), "L")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("M",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Lodgement")> M_Criteria )
                                                    & (col("BusinessRelationship")=="Business"), "M")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("O",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("Teller") == "ATM")
                                                    & (col("positive_total_trans_amount") > O_Criteria), "O")
                                               .otherwise(None))


member_trans_aml = member_trans_aml.withColumn("P",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>P_Criteria)
                                                    & (lower(col("Nationality")).isin("russia","ukraine")) , "P")
                                               .otherwise(None))

member_trans_aml = member_trans_aml.withColumn("Q",
                                               when((col("Codes_Not_To_USE")=="YES")
                                                    & (col("positive_total_trans_amount")>Q_Criteria)
                                                    & (lower(col("Nationality")).isin(residency_countries)) , "Q")
                                               .otherwise(None))

not_null_criteria = member_trans_aml.select("A","B", "C", "E", "F", "G", "J", "K", "L", "M", "O","P","Q")
final_criteria = member_trans_aml.select("No","Teller","Transaction Detail","Transaction Number","C/D",
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
                                         "Debit Card","sub_office",
                                         "A","B", "C", "E", "F", "G", "J", "K", "L", "M", "O","P","Q")


#These name should be added later
#"R_Level"
# Personal/Business
# MoneyMule Acc   Combined Acc "Residency",
#"Date & Time",






c_s3_bucket_path = "s3://alm-platform-bucket/source/demo/criteria.csv"

condition = coalesce(*[col(c) for c in not_null_criteria.columns]) != lit("")
df_filtered = final_criteria.filter(condition)
df_filtered.coalesce(1).write.mode("overwrite").option("header", "true").csv(c_s3_bucket_path)





# final_criteria.where(final_criteria['Transaction Number'] == 19302581).show()


counts_of_A = member_trans_aml.filter(member_trans_aml['A'] == 'A').count()
print("Count of records where column 'A' equals 'A':", counts_of_A)
print("==========================================================")

counts_of_B = member_trans_aml.filter(member_trans_aml['B'] == 'B').count()
print("Count of records where column 'B' equals 'B':", counts_of_B)
print("==========================================================")


counts_of_C = member_trans_aml.filter(member_trans_aml['C'] == 'C').count()
print("Count of records where column 'C' equals 'C':", counts_of_C)
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




