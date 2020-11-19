from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.append_data=append_data
        
        def execute(self, context):
            self.log.info('LoadDimensionOperator has been implemented !')
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
            if self.append_data == True:
                load_sql_stmt = 'INSERT INTO %s %s' % (self.table, self.load_sql_stmt)
                redshift.run(load_sql_stmt)
            else:
                load_sql_stmt = 'TRUNCATE TABLE %s;' % (self.table)
                load_sql_stmt =load_sql_stmt + 'INSERT INTO %s %s' % (self.table, self.load_sql_stmt)
                redshift.run(load_sql_stmt)       
