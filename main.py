import os
import time
from db import create_db, create_file, extract_file, get_databases, restore_postgres_db, showDBs
from send_drive import download_file, list_items, searchFile, uploadFile
from tools import printProgressBar


def do_one_backup(name):
    f = open("logs.txt", "a")
    databases = get_databases()
    l = len(databases)
    printProgressBar(0, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
    index = 0
    for database in databases:
        index+=1
        db, owner = database
        if db == name:
            file_name, gz_file = create_file(db, owner)
            time.sleep(1)
            os.remove(file_name)
            id_drive_db = uploadFile(gz_file, gz_file, "application/gzip")
            f.write(f"{index}: {db} \t=> {id_drive_db}\n")
            os.remove(gz_file)
            time.sleep(1)
        printProgressBar(index, l, prefix = 'Progress:', suffix = 'Complete', length = 50)

    f.close()

def do_backups():
    f = open("logs.txt", "a")
    databases = get_databases()
    l = len(databases)
    printProgressBar(0, l, prefix = 'Progress:', suffix = 'Complete', length = 50)
    index = 0
    for database in databases:
        index+=1
        db, owner = database
        file_name, gz_file = create_file(db, owner)
        time.sleep(1)
        os.remove(file_name)
        id_drive_db = uploadFile(gz_file, gz_file, "application/gzip")
        f.write(f"{index}: {db} \t=> {id_drive_db}\n")
        os.remove(gz_file)
        time.sleep(1)
        printProgressBar(index, l, prefix = 'Progress:', suffix = 'Complete', length = 50)

    f.close()

def restore_backups(names, soft_type):
    for name in names:
        db_name = f"db_{name}.backup.gz" if soft_type=='1' else f"{name}_db.backup.gz"
        print("1", db_name)
        file_id, file_name = searchFile(100, db_name, '', 'id')
        print(file_id, file_name)
        download_file(file_id, file_name)
        backup_file = extract_file(f'./bk/{file_name}')
        db = create_db(name, soft_type)
        restore_postgres_db(db, backup_file)

def main():
    print("\nOPTIONS:")
    print("1.- Show DataBases")
    print("2.- Generate One Backup")    
    print("3.- Generate Backups")    
    print("4.- List Backups drive")    
    print("5.- Download Backup drive")    
    print("6.- Download and Restore Backup drive")    
    print("0.- Exit")    
    option = int(input("Enter a number:"))

    if option == 1:
        showDBs()
        main()
    elif option == 2:
        name = input("Enter name:")
        do_one_backup(name)
        main()
    elif option == 3:
        do_backups()
        main()
    elif option == 4:
        list_items()
        main()
    elif option == 5:
        name = input("Enter name:")
        file_id, file_name = searchFile(100, name, '', 'id')
        download_file(file_id, file_name)
        # main()
    elif option == 6:
        names = input("Enter name(s):")
        names = list(map(str,names.split(' ')))
        mode = input("zenda(1) o flizzy(2): ")
        restore_backups(names, mode)
        # main()
    elif option == 0:
        print("Exit")
    else:
        print("Opcion no valida")
        main()

if __name__ == '__main__':
    main()