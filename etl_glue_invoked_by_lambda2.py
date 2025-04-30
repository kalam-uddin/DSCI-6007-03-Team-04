import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import gs_now
import gs_concat

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_SOURCE_PATH", "S3_DESTINATION_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1745879908665 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": [args["S3_SOURCE_PATH"]], "recurse": True}, transformation_ctx="AmazonS3_node1745879908665")

# Script generated for node Concatenate Columns
ConcatenateColumns_node1745880009644 = AmazonS3_node1745879908665.gs_concat(colName="claimlinenumber", colList=["CLAIM_ID", "CLAIM_LINE_ID"], spacer="-")

# Script generated for node Drop Fields
DropFields_node1745880054240 = DropFields.apply(frame=ConcatenateColumns_node1745880009644, paths=["CH_MEMBER_ID", "CLAIM_ID", "CLAIM_LINE_ID", "CH_EMPLOYER_ID", "PROVIDER_NPI", "FACILITY_NPI", "BILLING_PROVIDER_NPI", "PROVIDER_TIN", "SERVICE_PROVIDER_ID", "CHECK_NUMBER"], transformation_ctx="DropFields_node1745880054240")

# Script generated for node Rename Field
RenameField_node1745880141411 = RenameField.apply(frame=DropFields_node1745880054240, old_name="SOURCEFILENAME", new_name="SRCFILENAME", transformation_ctx="RenameField_node1745880141411")

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1745880179772 = RenameField_node1745880141411.gs_now(colName="imported_date", dateFormat="yyyyMMddHHmmss")

# Script generated for node SQL Query
SqlQuery1697 = '''
select * from source

'''
SQLQuery_node1745880200080 = sparkSqlQuery(glueContext, query = SqlQuery1697, mapping = {"source":AddCurrentTimestamp_node1745880179772}, transformation_ctx = "SQLQuery_node1745880200080")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745880200080, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745876244777", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745880209455 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1745880200080, connection_type="s3", format="glueparquet", connection_options={"path": args["S3_DESTINATION_PATH"], "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1745880209455")

job.commit()
