import logging as log
import requests as rest
import json
from datetime import datetime as dtm
from datetime import timedelta as tdm
import odbc
import config
import ms_sql as mssql
import time
import additional as adt


def create_body_requests(url, key, model, method, properties):
    _json = {
        "apiKey": key,
        "modelName": model,
        "calledMethod": method,
        "methodProperties": properties
    }
    return {
        'method': "POST",
        'url': url,
        'params': [],
        'json': _json
    }


def coll_rest(body_re):

    if body_re['method'] == "GET":
        response = rest.get(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "POST":
        response = rest.post(url=body_re['url'], params=body_re['params'], json=body_re['json'])

    return {
        'code': response.status_code,
        'headers': response.headers,
        'cookies': response.cookies,
        'text': response.text,
        'json': response.json()
    }


def assemble_model(row):
    datetime = dtm.now()
    return {
        "table": "dir_novaposhta",
        "uptime": str(datetime.strftime("%Y-%m-%dT%H:%M:%S." + str(datetime.microsecond)[0:3])),
        "actual": row.get('actual', 1),
        "type": row['type'],
        "coatsu": row.get('coatsu', ''),
        "description": row['Description'].replace("'", "`"),
        "ref": row['Ref'],
        "owner": row.get('ref_owner', ''),
        "json": json.dumps(row, ensure_ascii=False).replace("'", "`")
    }


def update_catalog(url, key, connect):
    log.info("Launching a directory update NP" + str(dtm.now()))
    get_areas(url, key, connect, True)
    log.info("Completion of the directory update NP" + str(dtm.now()))


def get_areas(url, key, connect, full):
    if connect is None:
        connect = mssql.open_connect(config.database_settings)['connect']
    get_type_dir(url, key, "", connect, "Address", "getAreas", {}, "area", full)


def get_cities(url, key, area_ref, connect, full):
    get_type_dir(url, key, area_ref, connect, "Address", "getCities", {"AreaRef": area_ref}, "сity", full)


def get_street(url, key, city_ref, connect):
    get_type_dir(url, key, city_ref, connect, "Address", "getStreet", {"CityRef": city_ref}, "street", False)


def get_warehouses(url, key, city_ref, connect):
    get_type_dir(url, key, city_ref, connect, "Address", "getWarehouses", {"CityRef": city_ref}, "warehouses", False)


def get_type_dir(url, key, ref_owner, connect, model, method, properties, dir_type, full):
    data_model = []
    result = coll_rest(create_body_requests(url, key, model, method, properties))
    if result['code'] == 200:
        if result['json']['success']:
            if len(result['json']['data']) > 0:
                finish_ref = result['json']['data'][-1]['Ref']
                counter = 1
                for row in result['json']['data']:
                    row['type'] = dir_type
                    row['ref_owner'] = ref_owner
                    data_model.append(assemble_model(row))
                    if counter == 250 or row['Ref'] == finish_ref:
                        odbc.write_to_database(connect, data_model)
                        if dir_type == "area":
                            for row_data in data_model:
                                time_start = time.time()
                                get_cities(url, key, row_data['ref'], connect, full)
                                print_status(time_start, "NP / " + str(row_data['description']))
                            acrality_control(connect)
                        elif dir_type == "сity" and full:
                            for row_data in data_model:
                                get_street(url, key, row_data['ref'], connect)
                                get_warehouses(url, key, row_data['ref'], connect)
                        data_model.clear()
                        counter = 1
                    else:
                        counter += 1
    else:
        log.warning("Warning: code=" + str(result['code']) + "}")


def acrality_control(connect):
    time_start = time.time()
    datetime = dtm.now() - tdm(days=1)
    uptime = str(datetime.strftime("%Y-%m-%dT%H:%M:%S." + str(datetime.microsecond)[0:3]))
    request_text = "UPDATE dir_novaposhta SET actual = 0 WHERE uptime <= '" + str(uptime) + "'"
    odbc.write_to_database(connect, request_text)
    print_status(time_start, "NP Stop  / acrality / ")


def print_status(time_start, text):
    if config.print_status:
        print(text + " : " + adt.calculate_time(time_start))
