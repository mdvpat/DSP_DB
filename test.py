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

df_bdd_return =  func.requesting_bdd(commune, typologie, our_host, our_dbname, our_user, our_password, auth_plugin)
result = func.model_passing(df_bdd_return, surface)
df_bdd = df_bdd_return.to_dict('index')
print({"transactions": df_bdd, "result": result})