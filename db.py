import psycopg2
import gzip
import subprocess
import shutil
import json
import time

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

def write_file(filename, data):
    try:
        f = open(filename, "wb")
    except IOError as e:
        print(e.errno, e.message)
    else:
        f.write(data)
        f.close()

def decompress(filename):
    f = gzip.open(filename)
    write_file(filename[:filename.rfind(".gz")], f.read())
    f.close()

def restore_zenda(file_name, owner):
    decompress(f"db_{file_name}.backup.gz")
    if owner == 'postgres':
        cmd = f'pg_restore -h localhost -p 5432 -U postgres -d {file_name} -v "db_{file_name}.backup"'
    else:
        # cmd = f"pg_dump --dbname=postgresql://{owner}:{owner}@127.0.0.1:5432/{file_name} -f {file_name_bk}