import odbcd.connector as odbc_con
import datetime


def start_service():

    database_settings = {
            'type': 'MSQL',
            'server': '127.0.0.1',
            'username': 'sa',
            'password': '82371'
        }
    connect = odbc_con.open_connect(database_settings)
    if not connect['status']:
        print(connect['error'])

    connect = connect['connect']
    control = odbc_con.control_database(connect, "UDADS")
    if not control['status']:
        print(control['error'])

    while True:
        odbc_con.start_controller(connect, datetime.datetime.now().second)
        print(datetime.datetime.now().second)
