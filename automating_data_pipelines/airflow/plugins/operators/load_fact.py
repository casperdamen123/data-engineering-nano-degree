from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 creation_statement="",
                 insertion_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.creation_statement = creation_statement
        self.insertion_statement = insertion_statement

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Create fact table
        self.log.info(f"Fact table: {self.target_table} is being created if non existing")
        redshift.run(self.creation_statement)

        # Insert fact table
        self.log.info(f"Fact table: {self.target_table} is being inserted")
        sql_insertion = f"""
                        INSERT INTO {self.target_table}
                        {self.insertion_statement}
                        """
        redshift.run(sql_insertion)
        self.log.info(f"Fact table: {self.target_table} has been inserted")
