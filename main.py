import os
import psycopg2
import gzip
import subprocess
import shutil
import time

from send_drive import uploadFile

db_user = 'postgres'
db_pass = 'admin'
db_host = '127.0.0.1'
db_port = 5432


def __conectarse():
    try:
        cnx = psycopg2.connect( user=db_user, password=db_pass, host=db_host, port=db_port)
        return cnx
    except (Exception, psycopg2.Error) as error:
        print(f'Connection exception {error}')

def get_databases():    
    conn = __conectarse()
    cursor = conn.cursor()
    cursor.execute("SELECT dbs.datname, roles.rolname FROM pg_database dbs, pg_roles roles WHERE datistemplate = false and dbs.datdba = roles.oid and dbs.datname<>'postgres' and roles.rolname<>'postgres'and roles.rolname<>'comercial'and roles.rolname<>'subcafae';")
    return cursor.fetchall()

def showDBs():
    dbs = get_databases()
    count=0
    print("DATABASES\t=>\tOWNERS")
    for db in dbs:
        count+=1
        print(f"{count} {db[0]}\t=> {db[1]}")
    main()


def create_file(file_name, owner):
    file_name_bk = file_name +'.backup'
    gz_name = file_name_bk +'.gz'

    cmd = f"pg_dump --dbname=postgresql://{owner}:{owner}@127.0.0.1:5432/{file_name} -f {file_name_bk}"
    # print(cmd)

    with gzip.open(file_name_bk, 'wb') as f:
        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)    
    for stdout_line in iter(popen.stdout.readline, ""):
        f.write(stdout_line.encode('utf-8'))    
    popen.stdout.close()
    popen.wait()
    time.sleep(0.5)
    
    with open(file_name_bk, 'rb') as f_in:
        with gzip.open(gz_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return file_name_bk, gz_name

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

def do_backups():
    f = open("logs.txt", "a")
    databases = get_databases()
    l = len(databases)
    printProgressBar(0, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
    index = 0
    for database in databases:
        index+=1
        printProgressBar(index, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
        db, owner = database
        file_name, gz_file = create_file(db, owner)
        time.sleep(1)
        id_drive_db = uploadFile(gz_file, gz_file, "application/gzip")
        f.write(f"{index}: {db} \t=> {id_drive_db}\n")

        time.sleep(1)
        os.remove(file_name)
        os.remove(gz_file)
    f.close()

def main():
    print("\nOPTIONS:")
    print("1.- Show DataBases")
    print("2.- Generate Backups")    
    print("0.- Exit")    
    option = int(input("Enter a number:"))

    if option == 1:
        showDBs()
    elif option == 2:
        do_backups()
    elif option == 0:
        print("Exit")
    else:
        print("Opcion no valida")

if __name__ == '__main__':
    main()