from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, redshift_conn_id="", table="", sql="", insert_mode="", *args, **kwargs
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        """
        Insert data from staging table into Dimension Table
        """
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info("Inserting data into Dimension Table")

        try:
            redshift.run(self.sql)
            self.log.info(
                f"Successful data insertion into dimension table: {self.table}"
            )
        except Exception as e:
            self.log.error(f"Error inserting data: {str(e)}")
            raise e
