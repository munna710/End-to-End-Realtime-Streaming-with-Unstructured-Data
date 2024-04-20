from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from config.config import configuration
from udfs.udfs import *

def define_udfs():
    return {
        'extract_file_name_udf': udfs(extract_file_name, StringType()),
        'extract_position_udf': udfs(extract_position, StringType()),
        'extract_classcode_udf': udfs(extract_classcode, StringType()),
        'extract_salary_udf': udfs(extract_salary, DoubleType()),
        'extract_date_udf': udfs(extract_date, StringType()),
        'extract_requirements_udf': udfs(extract_requirements, StringType()),
        'extract_notes_udf': udfs(extract_notes, StringType()),
        'extract_duties_udf': udfs(extract_duties, StringType()),
        'extract_selection_udf': udfs(extract_selection, StringType()),
        'extract_education_length_udf': udfs(extract_education_length, StringType()),
        'extract_school_type_udf': udfs(extract_school_type, StringType()),
        'extract_experience_length_udf': udfs(extract_experience_length, StringType()),
        'extract_job_type_udf': udfs(extract_job_type, StringType()),
        'extract_application_location_udf': udfs(extract_application_location, StringType())
        }

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("AWS_Spark_Unstructured")
            .config('spark.jars,packages','org.apache.hadoop:hadoop-aws:3.3.1,''com.amazonaws:aws-java-sdk-bundle:1.11.469').config('spark.hadoop.fs.s3a.access.key','').config('spark.hadoop.fs.s3a.imp','org.apache.hadoop.fs.s3a.S3AFileSystem')
            .config('spark.hadoop.fs.s3a.access.key',configuration.get('AWS_ACCESS_KEY'))
            .config('spark.hadoop.fs.s3a.secret.key',configuration.get('AWS_SECRET_KEY'))
            .config('spark.hadoop.fs.s3a.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').getOrCreate())
    
    text_input_dir = 'input/input_text'
    json_input_dir = 'input/input_json'
    csv_input_dir = 'input/input_csv'
    pdf_input_dir = 'input/input_pdf'
    img_input_dir = 'input/input_img'
    video_input_dir = 'input/input_video'

    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('classcode', StringType(), True),
        StructField('salary_start', DoubleType(), True),
        StructField('salary_end', DoubleType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('education_lenghth', StringType(), True),
        StructField('school_type', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('application_location', StringType(), True),


    ])

    udf = define_udfs()
    job_bulletins_df = (spark.readStream
                        .format('text')
                        .option('wholetext','true')
                        .load(text_input_dir) )
    #job_bulletins_df.show()
    json_df = spark.readStream.json(json_input_dir, schema=data_schema, multiLine=True)

    job_bulletins_df = job_bulletins_df.withColumn('file_name', udf['extract_file_name_udf'](job_bulletins_df['value'])) 
    job_bulletins_df = job_bulletins_df.withColumn('value',regexp_replace('value',r'\n',' '))
    job_bulletins_df = job_bulletins_df.withColumn('position', udf['extract_position_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('salary', udf['extract_salary_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('start_date', udf['extract_date_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('end_date', udf['extract_date_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('req', udf['extract_requirements_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('notes', udf['extract_notes_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('duties', udf['extract_duties_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('selection', udf['extract_selection_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('education_length', udf['extract_education_length_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('school_type', udf['extract_school_type_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('experience_length', udf['extract_experience_length_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('job_type', udf['extract_job_type_udf'](job_bulletins_df['value']))
    job_bulletins_df = job_bulletins_df.withColumn('application_location', udf['extract_application_location_udf'](job_bulletins_df['value']))

    j_df = job_bulletins_df.select('file_name','position','salary','start_date','end_date','req','notes','duties','selection','education_length','school_type','experience_length','job_type','application_location')

    json_df = json_df.select('file_name','position','salary','start_date','end_date','req','notes','duties','selection','education_length','school_type','experience_length','job_type','application_location')

    union_dataframe = j_df.union(json_df)
    
    query = (job_bulletins_df
            .writeSteam
            .outputMode('append')
            .format('console')
            .start()
            )
    def streamWriter(df, output_dir):
        return(input.writeSteam.format('parquet')
        .option('checkpointLocation',checkpointFolder)
        .option('path',output)
        .outputMode('append')
        .trigger(processingTime='5 seconds')
        .start())
    query = streamWriter(union_dataframe, 's3a://spark-unstructured-streaming/checkpoints/','s3a://spark-unstructured-streaming/data/spark-unstructured')
    
    query.awaitTermination()