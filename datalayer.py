import psycopg2
import psycopg2.extras
from datetime import date
from os import path, makedirs


NAME ='ODetection'
USER = 'postgres'
PASSWORD = '2s3ASj@$pALG_aj'
HOST = 'localhost'
PORT = '5432'
SCHEMA_NAME = 'public' 



class VideoSQL:

    def __init__(self) -> None:
        self.TABLE_NAME = 'videopost'
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

    def insertFile(self, filePath):
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

            checkTableSQLStatement = f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY ,video_file VARCHAR(1000) NOT NULL, created date NOT NULL)"
            insertSQLStatement = f"INSERT INTO {self.TABLE_NAME} (video_file,created) VALUES (%s, %s)"
            
            with connection:
                with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(checkTableSQLStatement)
                    cursor.execute(insertSQLStatement,(filePath,today,))
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

class JsonSQL:

    def __init__(self) -> None:
        self.TABLE_NAME = 'posts_jsonfiles'
        self.file_type = {'.json'}
        self.myDirectory = 'media/json'

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

    def insertFile(self, filePath):
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
            
            #TODO remove this hardcoded part
            video_ID = input('For which video id?\n')
            model_ID = input('For which ML model id?\n')
            condition = input('which condition raw or modified?\n')
            #

            checkTableSQLStatement = f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY ,json_file VARCHAR(1000) NOT NULL,created date NOT NULL,condition VARCHAR(30) NOT NULL,ML_model_id_id INTEGER NOT NULL,video_id_id INTEGER NOT NULL)"
            insertSQLStatement = f"INSERT INTO {self.TABLE_NAME} (json_file,created, condition, ML_model_id_id,video_id_id) VALUES (%s, %s, %s, %s, %s)"
            
            with connection:
                with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(checkTableSQLStatement)
                    cursor.execute(insertSQLStatement,(filePath,today,condition,model_ID,video_ID,))
                    iserted_numbers = cursor.rowcount
                    print(f"{iserted_numbers} File inserted.\n")

    def retreiveFileByPath(self,filePath):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE json_file = '{str(filePath)}'"
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
                result_location = cursor.fetchone()['json_file']
                print(f"File uploaded from {result_location}.\n")
        
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
                    print(f"ID\t\t Location\t\t\t Created at\t\t condition\t\t ML_Model_ID\t\t Video_ID\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\t\t {res[2]}\t\t {res[3]}\t\t\t {res[4]}\t\t {res[5]}\n")
                        
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
                    print(f"ID\t\t Location\t\t\t Created at\t\t condition\t\t ML_Model_ID\t\t Video_ID\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\t\t {res[2]}\t\t {res[3]}\t\t\t {res[4]}\t\t {res[5]}\n")
    
    def retreiveJsonFilesVideoID(self,number_of_rows,video_id):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE video_id_id = '{str(video_id)}' ORDER BY created DESC LIMIT '{str(number_of_rows)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if len(result) > 0:
                    print(f"The last {number_of_rows} Files uploaded or modified:\n")
                    print(f"ID\t\t Location\t\t\t Created at\t\t condition\t\t ML_Model_ID\t\t Video_ID\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\t\t {res[2]}\t\t {res[3]}\t\t\t {res[4]}\t\t {res[5]}\n")

    def alterFile(self,ID,filepath):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        get='json_file'

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
        get='json_file'
        
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



while True:
    modeInput = int(input("\
        1 -> Working with Videos\n\
        2 -> Working with Json\n"))
    if modeInput == 1 or modeInput == 2:
        mode = {1:'Video', 2:'Json'}
        userInput = int(input(f"\
            1 -> Insert {mode[modeInput]}\n\
            2 -> Find {mode[modeInput]} by ID\n\
            3 -> Store {mode[modeInput]} locally\n\
            4 -> Alter {mode[modeInput]}\n\
            5 -> Find {mode[modeInput]} based on date\n\
            6 -> Find {mode[modeInput]} based on filepath\n\
            7 -> Find certain number of {mode[modeInput]}s\n\
            8 -> (Only for Videos) Find the related Json for Videos\n"))
        if modeInput == 1:
            obj = VideoSQL()
            obj_json = JsonSQL()
        elif modeInput == 2:
            obj = JsonSQL()

        if userInput == 1:
            userFilePath = input("Enter the input file path\t")
            obj.insertFile(userFilePath)

        elif userInput == 2:
            file_ID = input("Enter ID:\t")
            obj.retreiveFileByID(file_ID)

        elif userInput == 3:
            file_ID = input("Enter ID:\t")
            filename = input("Enter filename:\t")
            obj.storeFileLocally(file_ID,filename)

        elif userInput == 4:
            file_ID = input("Enter ID:\t")
            filename = input("New file path:\t")
            obj.alterFile(file_ID,filename)

        elif userInput == 5:
            from_Date = input("Enter from date (yyyy-mm-dd): \t")
            to_Date = input("Enter from date (yyyy-mm-dd):\t")
            obj.retreiveFileByDate(from_Date,to_Date)

        elif userInput == 6:
            userFilePath = input("Enter the input file path\t")
            obj.retreiveFileByPath(userFilePath)
        
        elif userInput == 7:
            num_rows = input("Enter the number of rows to be displayed\t")
            obj.retreiveFiles(num_rows)
        
        elif userInput == 8:
            video_ID = input("Enter Video ID:\t")
            num_rows = input("Enter the number of rows to be displayed\t")
            obj_json.retreiveJsonFilesVideoID(num_rows,video_ID)

        else:
            print('Wrong input') 
