from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    copy_sql = """
        COPY {}
        FROM '{}'
        REGION '{}'
        IAM_ROLE '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        aws_conn_id="",
        redshift_conn_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="",
        region="",
        iam_role="",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.iam_role = iam_role

    def execute(self):
        """
        Copy data from S3 to Redshift staging table
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            self.region,
            self.iam_role,
            self.json_path,
        )

        try:
            redshift.run(formatted_sql)
            self.log.info(f"Successfully copied data from {s3_path} to {self.table}")
        except Exception as e:
            self.log.error(f"Error copying data: {str(e)}")
            raise e
