import psycopg2
import csv

db_name = 'db_pipeline'
user = 'postgres'
password = '1234'
host = 'localhost'
port = '5432'

try:
    connection = psycopg2.connect(dbname=db_name,
                                  user=user,
                                  password=password,
                                  host=host,
                                  port=port)

    if connection:
        print(f"Connected to database {db_name}")
    cursor = connection.cursor()
except psycopg2.Error as error:
    print(f"Error to database {db_name}", error)


def connect(query, f=False):
    try:
        cursor.execute(query)
        if f:
            rows = cursor.fetchall()
            for row in rows:
                for i in row:
                    print(i)
    except psycopg2.Error as error_q:
        print(f"Error to database {db_name}", error_q)
        return False
    else:
        return True


# Copy file to table
def copy_file(file, table):
    # query = '''COPY ''' + table + ''' FROM ''' + file + ''' DELIMITER ',' CSV HEADER;'''
    # connect(query)
    try:
        query_create_table = ('CREATE TABLE original(id int NOT NULL, \n'
                              '                             name VARCHAR(50), \n'
                              '                             url VARCHAR(50));')
        cursor.execute(query_create_table)

        with open(file, 'r') as f:
            next(f)
            cursor.copy_from(f, table, sep=',')
        # query = "COPY " + table + " FROM %s DELIMITER ‘,’ CSV HEADER;"
        # cursor.execute(query,(file,))

    except psycopg2.Error as error:
        print(f"Error connecting to database {db_name}", error)
        return False
    else:
        return True


# Copy table to other table  WITH NO DATA
def create_table(table, task):
    try:
        query = ('CREATE TABLE ' + table + ' (id int NOT NULL, \n'
                                           'name varchar(50), \n'
                                           'url varchar(50), \n'
                                           'domain_of_url varchar(50));')
        cursor.execute(query)
        cursor.execute('select id, name, url from original')
        rows = cursor.fetchall()
        cur = connection.cursor()
        for row in rows:
            query = 'INSERT INTO ' + table + ' VALUES(%s,%s,%s,%s);'
            cur.execute('select split_part(%s, %s, 3);', (row[2], '/',))
            domain = cur.fetchone()
            cursor.execute(query, (row[0], row[1], row[2], domain[0],))
        # query = "COPY " + table + " FROM %s DELIMITER ‘,’ CSV HEADER;"
        # cursor.execute(query)

    except psycopg2.Error as error:
        print(f"Error connecting to database {db_name}", error)
        return False
    else:
        return True


# Copy table to file
def copy_table(table, file):

    query = ("COPY " + table + " TO %s CSV HEADER DELIMITER ',';")
    cursor.execute(query, (file,))

    # try:
    #     with open(file, 'w') as f:
    #         next(f)
    #         cursor.copy_to(f, table, sep=',')
    # except psycopg2.Error as error:
    #     print(f"Error connecting to database {db_name}", error)
    #     return False
    # else:
    #     return True

