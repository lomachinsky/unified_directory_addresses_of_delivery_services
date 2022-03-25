import requests
import ms_sql as mssql
from datetime import datetime, timedelta
import json




def coll(body_re):

    if body_re['method'] == "GET":
        response = requests.get(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "POST":
        response = requests.post(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "PUT":
        response = requests.put(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "PATCH":
        response = requests.patch(url=body_re['url'], params=body_re['params'], json=body_re['json'])
    elif body_re['method'] == "DELETE":
        response = requests.delete(url=body_re['url'], params=body_re['params'], json=body_re['json'])

    return {
        'code': response.status_code,
        'headers': response.headers,
        'cookies': response.cookies,
        'text': response.text,
        'json': response.json()
    }


def create_requests(url, key, model, method, properties):
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


def check_existence(connect, row):

    _table = "np_directory"
    _date = datetime.now()
    _uptime = "'"+str(_date.strftime("%Y-%m-%dT%H:%M:%S." + str(_date.microsecond)[0:3]))+"'"
    _actual = row.get('actual', 1)
    _type = "'" + row['type'] + "'"
    _coatsu = "'" + row.get('coatsu', "null") + "'"
    _description = "'" + row['Description'].replace("'", "`") + "'"
    _ref = "'" + row['Ref'] + "'"
    _owner = "'" + row.get('ref_owner', "null") + "'"
    _json = "'" + json.dumps(row, ensure_ascii=False).replace("'", "`") + "'"

    request_text = f'\
            IF EXISTS(SELECT ref FROM {_table} WHERE ref = {_ref})\
                UPDATE {_table} SET uptime = {_uptime} WHERE ref = {_ref}\
            ELSE\
                INSERT INTO {_table} (uptime,actual,type,coatsu,description,ref,ref_owner,json)\
                VALUES ({_uptime},{_actual},{_type},{_coatsu},{_description},{_ref},{_owner},{_json});\
        '
    # mssql.write_to_database(connect['connect'], request_text)
    return request_text


def update_catalog(url, key, connect):
    get_areas(url, key, connect, True)


def get_areas(url, key, connect, full):
    request_text = ""
    _data = []
    result = coll(create_requests(url, key, "AddressGeneral", "getAreas", {}))
    if result['code'] == 200:
        if result['json']['success']:
            for row in result['json']['data']:
                row['type'] = "area"
                _data.append(row)
                request_text = request_text + check_existence(connect, row)
            mssql.write_to_database(connect, request_text)
            if full:
                for row in _data:
                    get_cities(url, key, row['Ref'], connect, full)
            acrality_control(connect)


def get_cities(url, key, area_ref, connect, full):
    properties = {
        "AreaRef": area_ref,
        "Page": "1"
    }
    request_text = ""
    _data = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll(create_requests(url, key, "AddressGeneral", "getCities", properties))
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
                    _data.append(row)
                    request_text = request_text + check_existence(connect, row)
            else:
                break
        count += 1
        if count == 500:
            break
        mssql.write_to_database(connect, request_text)
        if full:
            for row in _data:
                get_street(url, key, row['Ref'], connect)
                get_warehouses(url, key, row['Ref'], connect)


def get_street(url, key, city_ref, connect):
    properties = {
        "CityRef": city_ref,
        "Page": "1"
    }
    request_text = ""
    _data = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll(create_requests(url, key, "AddressGeneral", "getStreet", properties))
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
                    _data.append(row)
                    request_text = request_text + check_existence(connect, row)
            else:
                break
        count += 1
        if count == 500:
            break
        mssql.write_to_database(connect, request_text)


def get_warehouses(url, key, city_ref, connect):
    properties = {
        "CityRef": city_ref,
        "Page": "1"
    }
    request_text = ""
    _data = []
    first_line = None
    count = 1
    while True:
        properties['Page'] = str(count)
        result = coll(create_requests(url, key, "Address", "getWarehouses", properties))
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
                    _data.append(row)
                    request_text = request_text + check_existence(connect, row)
            else:
                break
        count += 1
        if count == 500:
            break
        mssql.write_to_database(connect, request_text)


def acrality_control(connect):
    _date = datetime.now() - timedelta(days=1)
    _uptime = "'"+str(_date.strftime("%Y-%m-%dT%H:%M:%S." + str(_date.microsecond)[0:3]))+"'"
    request_text = f'UPDATE np_directory SET actual = 0 WHERE uptime <= {_uptime}'
    mssql.write_to_database(connect, request_text)
