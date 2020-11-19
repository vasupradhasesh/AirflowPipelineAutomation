from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    json_sqlstatement = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
    """

    csv_sqlstatement = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 JSONPaths="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.aws_credentials_id = aws_credentials_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.JSONPaths = JSONPaths

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift..")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        json_path = "s3://{}/{}".format(self.s3_bucket, self.JSONPaths) 
        if rendered_key=="song_data":
            json_path = self.JSONPaths

        if self.file_type == "json":
            formatted_sql = StageToRedshiftOperator.json_sqlstatement.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            redshift.run(formatted_sql)

        if self.file_type == "csv":
            formatted_sql = StageToRedshiftOperator.csv_sqlstatement.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_sql)



