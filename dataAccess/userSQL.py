import psycopg2
import psycopg2.extras
from sys import maxsize

from .Postgres import *

# TODO PASSWORD TO BE ENCRYPTED

class UserSQL:

    def __init__(self) -> None:
        self.TABLE_NAME = 'posts_user_1'
    
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


    def createTable(self):
        if self.checkTableExistance():                    
            print("Table already exists!\n")
            return
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"CREATE TABLE {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY,\
                                                                    username VARCHAR(150) NOT NULL UNIQUE,\
                                                                    password VARCHAR (40) NOT NULL)"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                print(f"Table added.\n")


    def insert(self, username,password):

        if not self.checkTableExistance():                    
            self.createTable()

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        # checkTableSQLStatement = f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY ,username VARCHAR(150) NOT NULL UNIQUE, password VARCHAR (40) NOT NULL)"
        insertSQLStatement = f"INSERT INTO {self.TABLE_NAME} (username, password) VALUES (%s, %s)"
        
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(insertSQLStatement,(username,password,))
                iserted_numbers = cursor.rowcount
                print(f"{iserted_numbers} User added.\n")


    def retreiveByID(self,ID):
        result = None
        if not self.checkTableExistance():
            print("No such Table!\n")
            return result

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchone()
                print(f"ID\t\t  Username\n")
                print(f"{result[0]}\t\t{result[1]} \n")
        
        return result
    
    def retreiveByUsername(self,username):
        result = None
        if not self.checkTableExistance():
            print("No such Table!\n")
            return result

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE username = '{str(username)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchone()
                print(f"ID\t\t  Username\n")
                print(f"{result[0]}\t\t{result[1]} \n")
        
        return result
    
    def checkUserPass(self,username,password):
        result = False
        if not self.checkTableExistance():
            print("No such Table!\n")
            return result

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE username = '{str(username)}' AND password = '{str(password)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchone()
                if result and len(result) > 0 :
                    print(f"ID\t\t  Username\n")
                    print(f"{result[0]}\t\t{result[1]} \n")
                    return True
        
        return (result if result else False)


    def retreive(self,number_of_rows=maxsize):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} ORDER BY id LIMIT '{str(number_of_rows)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchall()
                if len(result) > 0:
                    print(f"The last users uploaded or modified:\n")
                    print(f"ID\t\t Username\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\n")
        return result

    def alterPassword(self,username,password,newPassword):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"UPDATE {self.TABLE_NAME} SET password = '{str(newPassword)}' WHERE username = '{str(username)}' AND password = '{str(password)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                renaming_numbers = cursor.rowcount
                print(f"{renaming_numbers} user names updated.\n")


