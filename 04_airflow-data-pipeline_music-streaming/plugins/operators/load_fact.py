from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self, redshift_conn_id="", sql="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self):
        """
        Insert data from staging table into Songplay Fact Table
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting data into Songplay Fact Table")

        try:
            redshift.run(self.sql)
            self.log.info("Successful data insertion")
        except Exception as e:
            self.log.error(f"Error inserting data: {str(e)}")
            raise e
