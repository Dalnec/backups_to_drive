import os
import time
from db import create_file, get_databases, showDBs
from send_drive import download_file, list_items, searchFile, uploadFile
from tools import printProgressBar


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

def main():
    print("\nOPTIONS:")
    print("1.- Show DataBases")
    print("2.- Generate Backups")    
    print("3.- List Backups drive")    
    print("4.- Download Backup drive")    
    print("0.- Exit")    
    option = int(input("Enter a number:"))

    if option == 1:
        showDBs()
        main()
    elif option == 2:
        do_backups()
        main()
    elif option == 3:
        list_items()
        main()
    elif option == 4:
        name = input("Enter name:")
        file_id, file_name = searchFile(100, name, '', 'id')
        download_file(file_id, file_name)
        # main()
    elif option == 0:
        print("Exit")
    else:
        print("Opcion no valida")
        main()

if __name__ == '__main__':
    main()