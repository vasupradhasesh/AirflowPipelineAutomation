import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 check_sql="",
                 expected_value="",
                 describe="",
                 tables=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.check_sql = check_sql
        self.expected_value = expected_value
        self.describe = describe
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("\n".join([
            'DataQuality check',
            self.describe,
            'expected value is {}'.format(self.expected_value),
            self.check_sql
        ]))
        records = redshift_hook.get_records(self.check_sql)
        if (records[0][0] < 1): # len(records) < 1 or len(records[0][0])
            raise ValueError(f"Data quality check failed, returned no results")
        if int(self.expected_value) != records[0][0]:
            raise ValueError(f"Data quality check failed. \n expected: {self.expected_value} \n acutal: {records[0][0]}")
        self.log.info(f"Data quality on \n {self.describe} \n check passed with \n expected: {self.expected_value} \n acutal: {records[0][0]}")
        
        for table in self.tables:
            self.log.info(f"Data Quality check for {table} table")
            tablerecords = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(tablerecords) < 1 or len(tablerecords[0]) < 1:
                raise ValueError(f"Data quality check failed, {table} returned no results")
            num_records = tablerecords[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed, {table} contained no rows")
                self.log.info(f"Data quality on table {table} check passed, returned {tablerecords[0][0]} records")