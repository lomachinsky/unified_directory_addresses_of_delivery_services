import re


def get_create_script_table(table_name):

    if table_name == "task_list":
        body_table = '\
            id INT IDENTITY NOT NULL,\
            name VARCHAR(99),\
            make bit,\
            model VARCHAR(99),\
            schedule INTEGER,\
            condition INTEGER,\
            date_create DATETIME, \
            date_start DATETIME, \
            date_stop DATETIME, \
            PRIMARY KEY(id)\
        '
    elif table_name == "schedule":
        body_table = '\
            id INT IDENTITY NOT NULL,\
            daily bit,\
            frequency_seconds INTEGER,\
            start_time INTEGER,\
            PRIMARY KEY(id)\
        '

    body_table = body_table.replace("\n", "")
    return "CREATE TABLE " + table_name + " (" + body_table + ");"
