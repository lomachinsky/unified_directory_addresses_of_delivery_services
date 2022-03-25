import logging as log
import requests as rest
import json
from datetime import datetime as dtm
from datetime import timedelta as tdm
import odbc
import config
import odbcd.ms_sql as mssql
import time


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
    elif body_re['method'] == "PUT":
        response = rest.put(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "PATCH":
        response = rest.patch(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "DELETE":
        response = rest.delete(url=body_re['url'], params=body_re['params'], json=body_re['json'])

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
    counter = 1
    data_model = []
    result = coll_rest(create_body_requests(url, key, "Address", "getAreas", {}))
    if result['code'] == 200:
        if result['json']['success']:
            for row in result['json']['data']:
                row['type'] = "area"
                data_model.append(assemble_model(row))
            odbc.write_to_database(connect, data_model)
            if full:
                for row in data_model:
                    start = time.time()
                    get_cities(url, key, row['ref'], connect, full)
                    print("NP / " + str(counter) + " / " + str(int(time.time() - start)))
                    counter += 1
            acrality_control(connect)
    else:
        log.warning("Warning: code=" + str(result['code']) + "}")


def get_cities(url, key, area_ref, connect, full):
    properties = {
        "AreaRef": area_ref,
        "Page": "1"
    }
    data_model = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll_rest(create_body_requests(url, key, "Address", "getCities", properties))
        if result['code'] == 200:
            if result['json']['success']:
                if len(result['json']['data']) == 0:
                    break
                for row in result['json']['data']:
                    if first_line == row:
                        break
                    row['type'] = "сity"
                    row['ref_owner'] = area_ref
                    first_line = row
                    data_model.append(assemble_model(row))
            else:
                break
        else:
            log.warning("Warning: code=" + str(result['code']) + "}")
        count += 1
        if count == 500:
            break
        odbc.write_to_database(connect, data_model)
        if full:
            for row in data_model:
                get_street(url, key, row['ref'], connect)
                get_warehouses(url, key, row['ref'], connect)
        data_model.clear()


def get_street(url, key, city_ref, connect):
    properties = {
        "CityRef": city_ref,
        "Page": "1"
    }
    data_model = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll_rest(create_body_requests(url, key, "Address", "getStreet", properties))
        if result['code'] == 200:
            if result['json']['success']:
                if len(result['json']['data']) == 0:
                    break
                for row in result['json']['data']:
                    if first_line == row:
                        break
                    row['type'] = "street"
                    row['ref_owner'] = city_ref
                    first_line = row
                    data_model.append(assemble_model(row))
            else:
                break
        else:
            log.warning("Warning: code=" + str(result['code']) + "}")
        count += 1
        if count == 500:
            break
        odbc.write_to_database(connect, data_model)
        data_model.clear()


def get_warehouses(url, key, city_ref, connect):
    properties = {
        "CityRef": city_ref,
        "Page": "1"
    }
    data_model = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll_rest(create_body_requests(url, key, "Address", "getWarehouses", properties))
        if result['code'] == 200:
            if result['json']['success']:
                if len(result['json']['data']) == 0:
                    break
                for row in result['json']['data']:
                    if first_line == row:
                        break
                    row['type'] = "warehouse"
                    row['ref_owner'] = city_ref
                    first_line = row
                    data_model.append(assemble_model(row))
            else:
                break
        else:
            log.warning("Warning: code=" + str(result['code']) + "}")
        count += 1
        if count == 500:
            break
        odbc.write_to_database(connect, data_model)
        data_model.clear()


def acrality_control(connect):
    print("NP Start / acrality / " + str(dtm.now()))
    datetime = dtm.now() - tdm(days=1)
    uptime = str(datetime.strftime("%Y-%m-%dT%H:%M:%S." + str(datetime.microsecond)[0:3]))
    request_text = "UPDATE dir_novaposhta SET actual = 0 WHERE uptime <= '" + str(uptime) + "'"
    odbc.write_to_database(connect, request_text)
    print("NP Stop  / acrality / " + str(dtm.now()))
