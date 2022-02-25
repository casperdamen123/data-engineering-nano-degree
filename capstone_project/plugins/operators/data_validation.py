from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataValidationOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 null_columns=(),
                 pos_columns=(),
                 *args, **kwargs):

        super(DataValidationOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.null_columns = null_columns
        self.pos_columns = pos_columns

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Verify is table has records
        record_check = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(record_check) < 1 or len(record_check[0]) < 1:
            raise ValueError(f"Data validation failed. {self.table} returns no results")
        num_records = record_check[0][0]
        if num_records < 1:
            raise ValueError(f"Data validation failed. {self.table} contains 0 rows")

        # Verify if columns have no null values
        for column in self.null_columns:
            null_check = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {column} IS NULL")
            if len(null_check) < 1 or len(null_check[0]) < 1:
                raise ValueError(f"Data validation failed. {column} not available")
            null_records = null_check[0][0]
            if null_records > 0:
                raise ValueError(f"Data validation failed. {column} contains {null_records} rows with NULL values")

        # Verify if columns values are positive
        for column in self.pos_columns:
            pos_check = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table} WHERE {column} < 0")
            if len(pos_check) < 1 or len(pos_check[0]) < 1:
                raise ValueError(f"Data validation failed. {column} not available")
            neg_records = pos_check[0][0]
            if neg_records > 0:
                raise ValueError(f"Data validation failed. {column} contains {neg_records} with positive values")

        self.log.info(f"Data validation on table {self.table} check passed")
