import pyodbc
import odbc.table_creation_scripts as tcs
import datetime
from threading import Thread


def open_connect(database_settings):
    text_error = 'Error database settings type'
    if database_settings['type'] == "MSQL":  # Microsoft SQL Server
        driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
        server = 'SERVER=' + database_settings['server']
        username = 'UID=' + database_settings['username']
        password = 'PWD=' + database_settings['password']
        try:
            connect = pyodbc.connect('' + driver + ';' + server + ';' + username + ';' + password, autocommit=True)
            return {'status': True, 'connect': connect.cursor()}
        except Exception as text_exception:
            text_error = text_exception

    return {'status': False, 'error': text_error}


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
        connect.execute(tcs.get_create_script_table(table_name))
        connect.execute(tcs.get_create_script_table("schedule"))
        return {'status': True}
    except Exception as text_exception:
        text_error = text_exception

    return {'status': False, 'error': text_error}


def modify_record(connect, id_task, condition):
    current_datetime = datetime.datetime.now()
    time_format = str(current_datetime.strftime("%Y-%m-%dT%H:%M:%S." + str(current_datetime.microsecond)[0:3]))
    body_text = "condition = " + str(condition) + ", "
    if condition == 99:
        body_text = body_text + "date_start = '" + time_format + "', date_stop = NULL"
    elif condition == 20:
        body_text = body_text + "date_stop = '" + time_format + "'"

    connect.execute("UPDATE task_list SET " + str(body_text) + " WHERE id = " + str(id_task) + ";")


def start_controller(connect, second_start):
    connect.execute("SELECT * FROM task_list WHERE make = 1")
    for row in connect.fetchall():
        timer_task = datetime.datetime.now() - row.date_start
        if row.make == 1 and row.condition != 99:
            if row.model == "regular":
                current_thread = Thread(target=start_regular, args=(row, connect))
                current_thread.start()
        elif row.date_stop is None and timer_task.days > 0:
            modify_record(connect, row.id, 50)

    while datetime.datetime.now().second == second_start:
        None

    return True


def start_regular(row, connect):
    modify_this_task = False
    if row.schedule is not None:
        connect.execute("SELECT * FROM schedule WHERE id = " + str(row.schedule) + ";")
        if connect.rowcount == -1:
            schedule = connect.fetchone()
            if schedule.daily == 1:
                current_datetime = datetime.datetime.now()
                timer_task = current_datetime - row.date_start
                if schedule.frequency_seconds > 0:
                    if timer_task.seconds >= schedule.frequency_seconds:
                        start_method(row, connect)
                        modify_this_task = True
                        modify_record(connect, row.id, 99)
                elif schedule.start_time > 0:
                    cd_second = (current_datetime.hour * 3600) + (
                                current_datetime.minute * 60) + current_datetime.second
                    if row.date_start.date() != current_datetime.date() and cd_second >= timer_task.seconds:
                        start_method(row, connect)
                        modify_this_task = True
                        modify_record(connect, row.id, 99)

        if modify_this_task:
            modify_record(connect, row.id, 20)


def start_method(row, connect):
    print(row.name)
