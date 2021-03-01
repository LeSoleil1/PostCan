import psycopg2
import psycopg2.extras
from sys import maxsize

from .Postgres import *

class CompanySQL:

    def __init__(self) -> None:
        self.TABLE_NAME = 'posts_company'
    
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

    def insert(self, companyName):        
        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (id SERIAL NOT NULL PRIMARY KEY ,name VARCHAR(1000) NOT NULL)"
        insertSQLStatement = f"INSERT INTO {self.TABLE_NAME} (name) VALUES (%s)"
        
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                cursor.execute(insertSQLStatement,(companyName,))
                iserted_numbers = cursor.rowcount
                print(f"{iserted_numbers} Company added.\n")


    def retreiveByID(self,ID):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

        checkTableSQLStatement = f"SELECT * FROM {self.TABLE_NAME} WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                result = cursor.fetchone()
                print(f"ID\t\t Company Name\n")
                print(f"{result[0]}\t\t{result[1]} \n")
        
        return result

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
                    print(f"The last Companies uploaded or modified:\n")
                    print(f"ID\t\t name\n")
                    for res  in result:
                        print(f"{res[0]}\t\t {res[1]}\n")
        return result

    def alter(self,ID,companyName):
        if not self.checkTableExistance():
            print("No such Table!\n")
            return None

        connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)
        checkTableSQLStatement = f"UPDATE {self.TABLE_NAME} SET name = '{str(companyName)}' WHERE id = '{str(ID)}'"
        with connection:
            with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
                cursor.execute(checkTableSQLStatement)
                renaming_numbers = cursor.rowcount
                print(f"{renaming_numbers} Comany names updated.\n")


