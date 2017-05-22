from datetime import datetime

def getDict(headers, data):
    if data != None:
        dictionary = dict(zip(headers, data))
        return dictionary
    else:
        return None

def dataToString(data):
    if data == None:
        data = "NULL"
    else:
        data = "'" + str.replace(str(data), "'", "''") + "'"
    return data

def listToString(data):
    string = ""
    i = 0
    l = len(data) - 1
    for item in data:
        struct = dataToString(item[0])
        if i == l:
            string = string + struct
        else:
            string = string + struct + ", "
        i += 1
    return string

def dictToString(dict):
    headers = ""
    i = 0
    l = len(dict) - 1
    for item in dict:
        if i == l:
            headers = headers + str(item)
        else:
            headers = headers + str(item) + ", "
        i += 1
    return headers

def insertStr(dict):
    string = ""
    i = 0
    l = len(dict) - 1
    for key, item in dict.items():
        struct = dataToString(item)
        if i == l:
            string = string + struct
        else:
            string = string + struct + ", "
        i += 1
    return string

def updateStr(dict):
    string = ""
    i = 0
    l = len(dict) - 1
    for key, item in dict.items():
        struct = dataToString(item)
        if i == l:
            string = string + key + " = " + struct
        else:
            string = string + key + " = " + struct + ", "
        i += 1
    return string

def delQuery(pk_value, pk, table):
    query = "DELETE FROM {0} WHERE {1} = {2}".format(table, pk, pk_value)
    msg = "{0}: {1} deleted from the destination database.".format(datetime.now(), pk_value)
    return query, msg

def repQuery(dict01, dict02, table, pk):
    if dict02 == None:
        #INSERT INTO DESTINATION DB
        headers = dictToString(dict01)
        insert = insertStr(dict01)
        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(table, headers, insert)
        msg = "{0}: {1} inserted into the destination database.".format(datetime.now(), dict01[pk])
    else:
        #UPDATE DESTINATION DB
        update = updateStr(dict01)
        query = "UPDATE {0} SET {1} WHERE {2} = '{3}'".format(table, update, pk, dict01[pk])
        msg = "{0}: {1} updated in the destination database.".format(datetime.now(), dict01[pk])
    return query, msg