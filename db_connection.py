#=============================================================

#This script is in charge of creating a connection to a postgres database server
#and dealing with the possible errors during query runtime

#=============================================================

import functions
import sys

def createDbConnection(**kwargs):

    db_host = kwargs.get('db_host')
    db_user = kwargs.get('db_user')
    db_pass = kwargs.get('db_pass')
    db_name = kwargs.get('db_name')
    db_type = kwargs.get('db_type')

    if db_type == 'mysql':
        try:
            import MySQLdb
            db_connection = MySQLdb.connect(host=db_host, user=db_user, password=db_pass, database=db_name)
            db_connection.set_client_encoding("utf8")
        except Exception as e:

            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION "+str(e)
            functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
            sys.exit(errorMessage)

    elif db_type == 'postgres':
        try:
            import psycopg2
            db_connection = psycopg2.connect(host=db_host, user=db_user, password=db_pass, dbname=db_name)
            db_connection.set_client_encoding("utf8")
        except Exception as e:

            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION "+str(e)
            functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
            sys.exit(errorMessage)
    elif db_type == 'sqlite':
        try:
            import sqlite3
            db_connection = sqlite3.connect(db_host)
        except Exception as e:

            errorMessage = "ERR_CANT_CREATE_DB_CONNECTION "+str(e)
            functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
            sys.exit(errorMessage)

    return db_connection


def dbConnectionExecuteQuery(**kwargs):
    try:
        cursor = kwargs.get('connectionObject').cursor()
    except Exception as e:

        errorMessage = "ERR_CANT_CREATE_DB_CONNECTION Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_CREATE_CURSOR'"
        functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
        sys.exit(errorMessage)

    try:
        if len(kwargs.get('queryArgs')) == 0:
            cursor.execute(kwargs.get('query'))
        else:
            cursor.execute(kwargs.get('query'), kwargs.get('queryArgs'))
    except Exception as e:

        errorMessage = "ERR_CANT_EXECUTE_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_EXECUTE_CURSOR'"
        functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
        kwargs.get('connectionObject').rollback()
        sys.exit(errorMessage)

    try:
        kwargs.get('connectionObject').commit()
    except Exception as e:

        errorMessage = "ERR_CANT_COMMIT_CURSOR Caught exception '" + str(e) + "', on query reference '" + kwargs.get('queryReference') + "_COMMIT_CURSOR'"
        functions.verbose(outputMode=kwargs.get('errorOutputMode'), outputMessage=errorMessage, logName="db_connection")
        kwargs.get('connectionObject').rollback()
        sys.exit(errorMessage)

    if cursor.description is None:
        fetchedResults = {}
        returnArray = [fetchedResults, 0, kwargs.get('query'), None]
    else:
        fetchedResults = cursor.fetchall()
        returnArray = [fetchedResults, len(fetchedResults), kwargs.get('query'), cursor.lastrowid]

    cursor.close()
    return returnArray
