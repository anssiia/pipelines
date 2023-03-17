from db.database import connect, copy_file, create_table, copy_table


class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        copy_table(self.table, self.output_file)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        flag = copy_file(self.input_file, self.table)
        if flag:
            print(f"Load file `{self.input_file}` to table `{self.table}`")
        else:
            print(f"Not loaded file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        connect(self.sql_query,0)
        print(f"Run SQL ({self.title}):\n{self.sql_query}")


class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        flag = create_table(self.table, self.sql_query)
        if flag:
            print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
        else:
            print(f"Not create table `{self.table}` as SELECT:\n{self.sql_query}")
