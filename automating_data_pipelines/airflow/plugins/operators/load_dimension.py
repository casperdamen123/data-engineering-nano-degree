from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table_to_insert="",
                 target_table="",
                 append_mode=False,
                 creation_statement="",
                 insertion_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.append_mode = append_mode
        self.creation_statement = creation_statement
        self.insertion_statement = insertion_statement

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create dimension table
        self.log.info(f"Dimension table: {self.target_table} is being created is non existing")
        redshift.run(self.creation_statement)

        # Insert or overwrite dimension table
        self.log.info(f"Dimension table: {self.target_table} is being inserted")

        if self.append_mode:
            sql = f"""
                   INSERT INTO {self.target_table}
                   {self.insertion_statement}
                   """
        else:
            sql = f"""
                   TRUNCATE TABLE {self.target_table};
                   INSERT INTO {self.target_table} 
                   {self.insertion_statement}
                   """

        redshift.run(sql)
        self.log.info(f"Dimension table: {self.target_table} has been inserted")
