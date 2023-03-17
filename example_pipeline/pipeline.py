from pipelines import tasks, Pipeline

NAME = 'test_project'
VERSION = '2023'

TASKS = [
    tasks.LoadFile(input_file='./original/original.csv', table='original'),
    tasks.RunSQL('select * from original'),
    tasks.CTAS(
        table='norm',
        sql_query='''
            select *, domain_of_url(url)
            from original;
        '''
    ),
    tasks.RunSQL('select * from norm'),
    # tasks.CopyToFile(
    #     table='norm',
    #     output_file="C:/Users/sz230/PycharmProjects/pipelines/norm.csv",
    # ),
    #
    # # clean up:
    tasks.RunSQL('drop table original'),
    tasks.RunSQL('drop table norm'),
]

pipeline = Pipeline(
    name=NAME,
    version=VERSION,
    tasks=TASKS
)

if __name__ == "__main__":
    # 1: Run as script
    pipeline.run()

    # 2: Run as CLI
    # > pipelines run
