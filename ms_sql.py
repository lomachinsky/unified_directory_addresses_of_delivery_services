import pyodbc


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


def open_connect(database_settings):
    try:
        connect = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};' +
            'SERVER=' + database_settings['server'] + ';' +
            'UID=' + database_settings['username'] + ';' +
            'PWD=' + database_settings['password'],
            autocommit=True
        )
        connect.cursor()
        connect.execute("USE UDADS")
        return {'status': True, 'connect': connect}
    except Exception as text_exception:
        return {'status': False, 'error': text_exception}


def control_database(connect, database_name):
    connect.execute("SELECT name FROM sys.databases")
    for row in connect:
        if row.name == database_name:
            connect.execute("USE " + database_name)
            return {'status': True}
    try:
        connect.execute("CREATE DATABASE " + database_name)
        connect.execute("USE " + database_name)
        control_table(connect, "task_list")
        return {'status': True}
    except Exception as text_exception:
        text_error = text_exception
    return {'status': False, 'error': text_error}


def control_table(connect, table_name):
    connect.execute("SELECT name FROM sys.objects WHERE type in (N'U')")
    for row in connect.fetchall():
        if row.name == table_name:
            return {'status': True}
    try:
        connect.execute(get_create_script_table(table_name))
        connect.execute(get_create_script_table("schedule"))
        return {'status': True}
    except Exception as text_exception:
        text_error = text_exception
    return {'status': False, 'error': text_error}


def write_to_database(connect, request_text):
    connect.execute(request_text)
