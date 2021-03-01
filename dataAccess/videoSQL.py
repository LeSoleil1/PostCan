import psycopg2
import psycopg2.extras
from datetime import date
from os import path, makedirs

from .Postgres import *

class VideoSQL:

    def __init__(self) -> None:
        self.TABLE_NAME = 'posts_videopost'
        self.file_type = {'.mp4', '.avi','.mov'}
        self.myDirectory = 'media/videos'

    def checkFileExist(self, filepath):
        return path.exists(filepath)

    def checkFileType(self, filepath):
        _ , fileExtension = path.splitext(filepath)
        return (fileExtension in self.file_type,fileExtension) 
    
    def checkTableExistance(self):
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT to_regclass('{SCHEMA_NAME}.{self.TABLE_NAME}')"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if result[0][0] == None:
                    return_flag = False # Hardcoded
                    print(f"Table doesn't exist!\n")     
                else:
                    return_flag = True
                    print("Table exists!\n")   
        return return_flag

    def insertFile(self, title, description, filePath, thumbnail, category, company_id, user_id):
        if not self.checkFileExist(filePath):
            print("No such file!\n")
            return
        checkFile,_ = self.checkFileType(filePath)
        
        if checkFile == False:
            print('Wrong file type\n')
            return 

        if self.checkTableExistance():                    
            if self.retreiveFileByPath(filePath) != None:
                print("File Cannot be added!\n")
                return
        
        with open(filePath, "rb") as file:
            binaryFileRep = file.read()
            connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
            today = date.today().strftime('%Y-%m-%d')

            checkTableSQLStatement = f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY, title TEXT NOT NULL ,description VARCHAR(100) NOT NULL ,video_file VARCHAR(100) NOT NULL, thumbnail VARCHAR(100), category VARCHAR(50) NOT NULL,created date NOT NULL, company_id_id INTEGER NOT NULL,user_id INTEGER NOT NULL)"
            insertSQLStatement = f"INSERT INTO {self.TABLE_NAME} (title, description,video_file,thumbnail, category,created,company_id, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s,%5)"
            
            with connection:
                with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(checkTableSQLStatement)
                    cursor.execute(insertSQLStatement,(title, description,filePath,thumbnail, category,today,company_id, user_id,))
                    iserted_numbers = cursor.rowcount
                    print(f"{iserted_numbers} File inserted.\n")

    def retreiveFileByPath(self,filePath):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE video_file = '{str(filePath)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if len(result) == 1:
                    result_id = result[0][0] # Hardcoded
                    print(f"File aleary uploaded with this ID: {result_id}\n")     
                else:
                    result_id = None
                    print("No file with this path stored in the database!\n")   
        return result_id
    
    def retreiveFileByID(self,ID):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result_location = cursor.fetchone()['video_file']
                print(f"File uploaded from {result_location}\n")
        
        return result_location

    def retreiveFileByDate(self,from_date,to_date):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE created BETWEEN  '{str(from_date)}' AND '{str(to_date)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if len(result) > 0:
                    print(f"File uploaded between {from_date} to {to_date} are:\n")
                    print(f"ID\t\t Location\t\t\t Created at\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\t\t {res[2]}\n")

    def retreiveFiles(self,number_of_rows):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} ORDER BY created DESC LIMIT '{str(number_of_rows)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if len(result) > 0:
                    print(f"The last {number_of_rows} Files uploaded or modified:\n")
                    print(f"ID\t\t Location\t\t\t Created at\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\t\t {res[2]}\n")
        return result

    def alterFile(self,ID,filepath):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        get='video_file'

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"UPDATE {self.TABLE_NAME} SET {get} = '{str(filepath)}' WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                reloaction_numbers = cursor.rowcount
                print(f"{reloaction_numbers} File locations updated.\n")

    def storeFileLocally(self,ID,filename):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        get='video_file'
        
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result_location = cursor.fetchone()[get]
                if len(result_location) > 0:
                    _ , FileExtension = self.checkFileType(result_location) # Assumption: Inserted correct video file
                    with open(result_location, "rb") as file_to_read:
                        binaryFileRep = file_to_read.read()
                        if not path.exists(self.myDirectory):
                            makedirs(self.myDirectory)
                        fullPath = self.myDirectory+'/'+filename
                        with open(fullPath,"wb") as file_to_write:
                            file_to_write.write(binaryFileRep)
                            file_to_write.close()
                            self.alterFile(ID,fullPath)
