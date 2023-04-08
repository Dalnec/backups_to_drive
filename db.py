import psycopg2
import gzip
import subprocess
import shutil
import json
import time
import os
import shlex
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


with open('config.json', 'r') as config_file:
    config = json.load(config_file)

db_user = config["db_user"] or 'postgres'
db_pass = config["db_pass"] or 'admin'
db_host = config["db_host"] or '127.0.0.1'
db_port = config["db_port"] or 5432


def __conectarse():
    try:
        cnx = psycopg2.connect( user=db_user, password=db_pass, host=db_host, port=db_port)
        return cnx
    except (Exception, psycopg2.Error) as error:
        print(f'Connection exception {error}')

def get_databases():    
    conn = __conectarse()
    cursor = conn.cursor()
    cursor.execute("SELECT dbs.datname, roles.rolname FROM pg_database dbs, pg_roles roles WHERE datistemplate = false and dbs.datdba = roles.oid and dbs.datname<>'postgres';")
    # cursor.execute("SELECT dbs.datname, roles.rolname FROM pg_database dbs, pg_roles roles WHERE datistemplate = false and dbs.datdba = roles.oid and dbs.datname<>'postgres' and roles.rolname<>'comercial'and roles.rolname<>'subcafae';")
    return cursor.fetchall()

def showDBs():
    dbs = get_databases()
    count=0
    print("DATABASES\t=>\tOWNERS")
    for db in dbs:
        count+=1
        print(f"{count} {db[0]}\t=> {db[1]}")


def create_file(file_name, owner):
    file_name_bk = file_name +'.backup'
    gz_name = file_name_bk +'.gz'

    if owner == 'postgres':
        # cmd = f"pg_dump --dbname=postgresql://postgres:{db_pass}@127.0.0.1:5432/{file_name} -f {file_name_bk}"
        cmd = f"PGPASSWORD='{db_pass}' pg_dump -d {file_name} -p 5432 -U postgres -h localhost -F t -f {file_name_bk}"
    else:
        # cmd = f"pg_dump --dbname=postgresql://{owner}:{owner}@127.0.0.1:5432/{file_name} -f {file_name_bk}"
        cmd = f"PGPASSWORD='{owner}' pg_dump -d {file_name} -p 5432 -U {owner} -h localhost -F t -f {file_name_bk}"

    with gzip.open(file_name_bk, 'wb') as f:
        popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)    
    for stdout_line in iter(popen.stdout.readline, ""):
        f.write(stdout_line.encode('utf-8'))    
    popen.stdout.close()
    popen.wait()
    time.sleep(0.5)
    
    with open(file_name_bk, 'rb') as f_in:
        with gzip.open(gz_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return file_name_bk, gz_name


def extract_file(src_file):
    extracted_file, extension = os.path.splitext(src_file)
    print(extracted_file)
    with gzip.open(src_file, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return extracted_file


def create_db(database, soft_type):
    if soft_type == '1': 
        db_name = f"db_{database}"
        user = 'zenda'
    else :
        db_name = f"{database}_db"
        user = 'restobar'
    try:
        conn = __conectarse()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE {} ;".format(db_name))
    except Exception as e:
        print('DB does not exist, nothing to drop')
    cursor.execute("CREATE DATABASE {} ;".format(db_name))
    cursor.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(db_name, user))
    return db_name


def restore_postgres_db(db, backup_file):
    """ Restore postgres db from a file. """
    try:
        with gzip.open(backup_file, 'rb') as f:
            process = subprocess.Popen(
                ['pg_restore',
                '--no-owner',
                '--dbname=postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db),
                '-v',
                backup_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))

        for stdout_line in iter(process.stdout.readline, ""):
            f.write(stdout_line.encode('utf-8'))    
            process.stdout.close()
            process.wait()
        return output
    except Exception as e:
        print("Issue with the db restore : {}".format(e))