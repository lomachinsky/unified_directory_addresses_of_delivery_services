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


def create_body_requests(url, key, method, model, body):
    return {
        'method': method,
        'url': url+model,
        'params': [],
        'headers': {
            'Authorization': 'Bearer ' + key,
            'Accept': 'application/json'
        },
        'json': body
    }


def coll_rest(body_re):

    if body_re['method'] == "GET":
        response = rest.get(url=body_re['url'], headers=body_re['headers'], params=body_re['params'])
    elif body_re['method'] == "POST":
        response = rest.post(url=body_re['url'], headers=body_re['headers'], params=body_re['params'])
    elif body_re['method'] == "PUT":
        response = rest.put(url=body_re['url'], headers=body_re['headers'], params=body_re['params'])
    elif body_re['method'] == "PATCH":
        response = rest.patch(url=body_re['url'], headers=body_re['headers'], params=body_re['params'])
    elif body_re['method'] == "DELETE":
        response = rest.delete(url=body_re['url'], headers=body_re['headers'], params=body_re['params'])

    return {
        'code': response.status_code,
        'headers': response.headers,
        'cookies': response.cookies,
        'text': response.text,
        'json': response.json()
    }


def assemble_model(row):

    datetime = dtm.now()

    coatsu = row.get('CITY_KOATUU', None)
    coatsu = row.get('REGION_KOATUU', None) if coatsu == 'None' else coatsu

    description = row.get('PO_LONG', None)
    description = row.get('STREET_UA', None) if description == 'None' else description
    description = row.get('CITY_UA', None) if description == 'None' else description
    description = row.get('REGION_UA', None) if description == 'None' else description
    description = description.replace("'", "`")

    ref = row.get('ID', None)
    ref = row.get('STREET_ID', None) if ref == 'None' else ref
    ref = row.get('CITY_ID', None) if ref == 'None' else ref
    ref = row.get('REGION_ID', None) if ref == 'None' else ref
    ref = ref.replace("'", "`")

    return {
        "table": "dir_ukrposhta",
        "uptime": str(datetime.strftime("%Y-%m-%dT%H:%M:%S." + str(datetime.microsecond)[0:3])),
        "actual": row.get('actual', 1),
        "type": row['type'],
        "coatsu": coatsu,
        "description": description,
        "ref": ref,
        "owner": row.get('ref_owner', ''),
        "json": json.dumps(row, ensure_ascii=False).replace("'", "`")
    }


def rebuild_string(row):
    return {
        "REGION_ID": str(row.get('REGION_ID', None)),
        "REGION_UA": str(row.get('REGION_UA', None)),
        "REGION_KOATUU": str(row.get('REGION_KOATUU', None)),
        "CITYTYPE_UA": str(row.get('CITYTYPE_UA', None)),
        "CITY_KOATUU": str(row.get('CITY_KOATUU', None)),
        "NAME_UA": str(row.get('NAME_UA', None)),
        "CITY_ID": str(row.get('CITY_ID', None)),
        "DISTRICT_UA": str(row.get('DISTRICT_UA', None)),
        "SHORTCITYTYPE_UA": str(row.get('SHORTCITYTYPE_UA', None)),
        "CITY_UA": str(row.get('CITY_UA', None)),
        "OWNOF": str(row.get('OWNOF', None)),
        "STREET_UA": str(row.get('STREET_UA', None)),
        "SHORTSTREETTYPE_UA": str(row.get('SHORTSTREETTYPE_UA', None)),
        "STREET_ID": str(row.get('STREET_ID', None)),
        "STREETTYPE_UA": str(row.get('STREETTYPE_UA', None)),
        "OLDSTREET_UA": str(row.get('OLDSTREET_UA', None)),
        "PO_LONG": str(row.get('PO_LONG', None)),
        "ID": str(row.get('ID', None))
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
    result = coll_rest(create_body_requests(url, key, "GET", "get_regions_by_region_ua", {}))
    if result['code'] == 200:
        if len(result['json']['Entries']) > 0:
            for row in result['json']['Entries']['Entry']:
                new_row = rebuild_string(row)
                new_row['type'] = "area"
                data_model.append(assemble_model(new_row))
            odbc.write_to_database(connect, data_model)
            if full:
                for row in data_model:
                    start = time.time()
                    get_cities(url, key, row['ref'], connect)
                    get_street(url, key, row['ref'], connect)
                    get_warehouses(url, key, row['ref'], connect)
                    print("UP / " + str(counter) + " / " + str(int(time.time() - start)))
                    counter += 1
            acrality_control(connect)
    else:
        log.warning("Warning: code=" + str(result['code']) + "}")


def get_cities(url, key, area_ref, connect):
    data_model = []
    model = "get_city_by_region_id_and_district_id_and_city_ua?region_id="+str(area_ref)
    result = coll_rest(create_body_requests(url, key, "GET", model, {}))
    if result['code'] == 200:
        if len(result['json']['Entries']) > 0 and len(result['json']['Entries']['Entry']) > 0:
            finish_city = result['json']['Entries']['Entry'][-1]['CITY_ID']
            counter = 1
            for row in result['json']['Entries']['Entry']:
                new_row = rebuild_string(row)
                new_row['type'] = "сity"
                new_row['ref_owner'] = area_ref
                data_model.append(assemble_model(new_row))
                if counter == 250 or row['CITY_ID'] == finish_city:
                    odbc.write_to_database(connect, data_model)
                    data_model.clear()
                    counter = 1
                else:
                    counter += 1
    else:
        log.warning("Warning: code=" + str(result['code']) + "}")


def get_street(url, key, area_ref, connect):
    data_model = []
    model = "get_street_by_region_id_and_district_id_and_city_id_and_street_ua?region_id=" + str(area_ref)
    result = coll_rest(create_body_requests(url, key, "GET", model, {}))
    if result['code'] == 200:
        if len(result['json']['Entries']) > 0 and len(result['json']['Entries']['Entry']) > 0:
            finish_street = result['json']['Entries']['Entry'][-1]['STREET_ID']
            counter = 1
            for row in result['json']['Entries']['Entry']:
                new_row = rebuild_string(row)
                new_row['type'] = "street"
                new_row['ref_owner'] = row['CITY_ID']
                data_model.append(assemble_model(new_row))
                if counter == 250 or row['STREET_ID'] == finish_street:
                    odbc.write_to_database(connect, data_model)
                    data_model.clear()
                    counter = 1
                else:
                    counter += 1
    else:
        log.warning("Warning: code=" + str(result['code']) + "}")


def get_warehouses(url, key, region_id, connect):
    data_model = []
    model = "get_postoffices_by_postindex?poRegionId=" + str(region_id)
    result = coll_rest(create_body_requests(url, key, "GET", model, {}))
    if result['code'] == 200:
        if len(result['json']['Entries']) > 0 and len(result['json']['Entries']['Entry']) > 0:
            finish_warehouses = result['json']['Entries']['Entry'][-1]['ID']
            counter = 1
            for row in result['json']['Entries']['Entry']:
                new_row = rebuild_string(row)
                new_row['type'] = "warehouse"
                new_row['ref_owner'] = row['PDCITY_ID']
                data_model.append(assemble_model(new_row))
                if counter == 250 or row['ID'] == finish_warehouses:
                    odbc.write_to_database(connect, data_model)
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
    request_text = "UPDATE dir_ukrposhta SET actual = 0 WHERE uptime <= '" + str(uptime) + "'"
    odbc.write_to_database(connect, request_text)
    print_status(time_start, "UP Stop  / acrality / ")


def print_status(time_start, text):
    if config.print_status:
        print(text + " : " + adt.calculate_time(time_start))
