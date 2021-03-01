import psycopg2
import psycopg2.extras


NAME ='ODetection'
USER = 'postgres'
PASSWORD = '2s3ASj@$pALG_aj'
HOST = 'localhost'
PORT = '5432'

connection = psycopg2.connect(dbname = NAME, user = USER, password = PASSWORD, host = HOST, port = PORT)

# cursor = connection.cursor()
cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)


# cursor.execute("CREATE TABLE testi (id SERIAL PRIMARY KEY, name VARCHAR);")
# cursor.execute("INSERT INTO testi (name) VALUES(%s)",("SOL",))

# cursor.execute("SELECT * FROM posts_videopost;")
# print(cursor.fetchall())

# cursor.execute("select current_date; -- date")
# print(cursor.fetchall())

# cursor.execute("SELECT * FROM posts_videopost WHERE created BETWEEN '2021-2-18' and '2021-2-18';")
# print(cursor.fetchall())

cursor.execute("SELECT video_file FROM posts_videopost WHERE created BETWEEN '2021-2-18' and '2021-2-18';")
print(cursor.fetchall())

# cursor.execute("SELECT * FROM testi WHERE id = %s;",(1,))
# print(cursor.fetchone())

# cursor.execute("SELECT * FROM posts_videopost WHERE id = %s;",(1,))
# print(cursor.fetchone()['created'])



connection.commit()

cursor.close()

# with connection:
#     with connection.cursor(cursor_factory = psycopg2.extras.DictCursor) as cursor:
#         cursor.execute("SELECT * FROM testi WHERE id = %s;",(1,))
#         print(cursor.fetchone()['name'])

connection.close()
