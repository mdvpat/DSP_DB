import pandas as pd
import pandas as pd
import mysql.connector
from mysql.connector import Error
import fonctions as func

our_host = "44.204.92.180"
our_dbname = "Projet3_DStest_LMJB"
our_user = 'root'
our_password = "admin2022"
auth_plugin = 'mysql_native_password'


connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)


commune = "AVIGNON"
surface = 100
typologie = "Maison"

df = func.requesting_bdd(commune, typologie, our_host, our_dbname, our_user, our_password, auth_plugin)
dic = func.model_passing(df, surface)
print({"my_df": df.to_dict("index"), "my_dic":dic})

'''

db_Info = connection.get_server_info()
print("Connected to MySQL Server version ", db_Info)
cursor = connection.cursor()
sql = "select * from DATASET where commune = '%s' and Surface_reelle_bati = %s and Nombre_pieces_principales = %s and Type_local = '%s'"%(commune, surface, nb_piece, typologie)
cursor.execute(sql)
#, code_postal, surface, nb_piece, typologie))
record = cursor.fetchall()
print(record)
cursor.close()
connection.close()
print("MySQL connection is closed")
'''

