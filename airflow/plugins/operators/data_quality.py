from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 redshift_conn_id="",
                 aws_credentials_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tables = tables
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for t in self.tables:
            records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(t))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(t))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(t))
            logging.info("Data quality on table {} check passed with {} records".format(t,records[0][0]))
            
    
        
        