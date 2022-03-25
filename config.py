
try:
    import ignor_file as ignor
except ValueError:
    print('Key file missing! / '+ValueError)

db_type = "mssql"
print_status = 1
database_settings = {
    'type': 'MSQL',
    'server': '127.0.0.1',
    'username': ignor.get_username(),
    'password': ignor.get_password()
}

settings_novaposhta = {
    "key": ignor.get_np_key(),
    "url": ignor.get_np_url()
}
settings_ukrposhta = {
    "key": ignor.get_up_key(),
    "url": ignor.get_up_url()
}
