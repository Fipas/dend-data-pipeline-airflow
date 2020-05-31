from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Performing data quality checks")
        
        for check in self.checks:
            sql = check.get("sql")
            expected_result = check.get("result")
            record = redshift.get_first(sql)
            if record is None or record[0] != expected_result:
                raise ValueError(f"Data quality check failed. Running: {sql}. Got: {record[0]}. Expected: {expected_result}")
        
        
        self.log.info("Data quality checks finished")