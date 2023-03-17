import psycopg2

db_name = 'db_pipeline'
user = 'postgres'
password = '1234'
host = 'localhost'
port = '5432'


class PostrgesDB:
    def __init__(self):
        try:
            self.connection = psycopg2.connect(dbname=db_name,
                                               user=user,
                                               password=password,
                                               host=host,
                                               port=port)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        except psycopg2.Error as error:
            print(f"Error connection to database {db_name}", error)

    # Query execution
    def run(self, query):
        try:
            self.cursor.execute(query)
        except psycopg2.Error as error:
            print(f"Error to database {db_name}", error)

    # Copy file to table
    def copy_file(self, input_file, table):
        try:
            query = f"COPY {table} FROM STDIN DELIMITER ',' CSV HEADER"
            self.cursor.copy_expert(query, open(input_file, "r"))
        except psycopg2.Error as error:
            print(f"Error connection to database {db_name}", error)

    # Copy table to file
    def copy_table(self, table, output_file):
        try:
            query = f"COPY (SELECT * FROM {table}) TO STDOUT DELIMITER ',' CSV HEADER"
            self.cursor.copy_expert(query, open(output_file, "w"))
        except psycopg2.Error as error:
            print(f"Error connection to database {db_name}", error)

    # Create function get domain from original table url
    def get_domain(self, url):
        try:
            query = f"CREATE OR REPLACE FUNCTION domain_of_url(url VARCHAR) RETURNS VARCHAR " \
                    "AS $$ " \
                    "BEGIN " \
                    "SELECT split_part({url},'/',3);" \
                    "END; " \
                    "$$ LANGUAGE plpgsql;"
            self.cursor.execute(query)
            # self.cursor.callproc("domain_of_url", [url, ])
        except psycopg2.Error as error:
            print(f"Error connection to database {db_name}", error)

    # Copy table to other table
    def create_table(self, table, task):
        try:
            query = f"CREATE TABLE IF NOT EXISTS {table} AS {task}"
            self.cursor.execute(query)
        except psycopg2.Error as error:
            print(f"Error connection to database {db_name}", error)

    # Close connection
    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
