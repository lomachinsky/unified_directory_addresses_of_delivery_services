import logging as log
import config as conf


def get_query_text_by_base_type(row):
    if conf.db_type == "mssql":
        return "\
                IF EXISTS(SELECT ref FROM " + row.get("table") + " WHERE ref = '" + row.get("ref") + "')\
                    UPDATE " + row.get("table") + " SET uptime = '" + row.get("uptime") + "' WHERE ref = '" + row.get(
            "ref") + "'\
                ELSE\
                    INSERT INTO " + row.get("table") + " (uptime,actual,type,coatsu,description,ref,ref_owner,json)\
                    VALUES ('" + row.get("uptime") + "','" + str(row.get("actual")) + "','" + row.get("type") + "',\
                        '" + row.get("coatsu") + "','" + row.get("description") + "','" + row.get("ref") + "',\
                        '" + row.get("owner") + "','" + row.get("json") + "');\
        "
    else:
        log.error("Database type not defined.")
        return None


def write_to_database(connect, data_model):

    if isinstance(data_model, str) is not True and len(data_model) == 0:
        log.error("Empty list.")

    request_text = ""
    if isinstance(data_model, str):
        request_text = data_model
    else:
        for row in data_model:
            request_text = request_text + get_query_text_by_base_type(row)

    if conf.db_type == "mssql":
        connect.execute(request_text)
    else:
        log.error("Database type not defined.")
