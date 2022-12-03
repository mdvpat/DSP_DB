import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

def pull_data():
    '''Fonction qui recupère les données d'après la bdd.
    Parcours chaque fichier pour absorber le csv du département'''
    rootdir = './bdd'
    path_list = []
    data= []
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            path_list.append(os.path.join(subdir, file))

    for e in path_list:
        if e[-3:] == 'txt':
            data_temp = pd.read_csv(e, sep='|')
            data.append(data_temp)

    df = pd.concat(data)


    df['Date mutation'] = pd.to_datetime(df['Date mutation'], dayfirst=True)
    df['annee'] = df['Date mutation'].dt.year
    #Adresse:
    my_address_variable = ['No voie', 'B/T/Q', 'Voie', 'Commune', 'Code postal']
    df = df[~df['Code postal'].isna()]
    df = df[~df['Commune'].isna()]
    df = df[~df['Voie'].isna()]

    df['Code département'] = df['Code departement'].astype('str')
    df['Code postal'] = df['Code postal'].astype('int')
    df['No voie'] = df['No voie'].astype('str')

    df['Code postal'] = df['Code postal'].apply(lambda x: '0'+str(x) if len(str(x))<5 else x)
    df['Code departement'] = df['Code departement'].apply(lambda x: '0'+str(x) if len(str(x))<2 else x)

    df['Adresse'] = df[my_address_variable].astype('str').apply(' '.join, axis=1)
    #df['Adresse'].replace('.0', '', inplace=True, regex=True)
    df['Adresse'].replace('nan', '', inplace=True, regex=True)
    df['Adresse'].replace('  ', ' ', inplace=True, regex=True)

    df['Valeur fonciere'] = df['Valeur fonciere'].str.replace(',', '.').astype('float')

    df = df[df['Nature mutation'].isin(['Vente', 'Vente terrain à bâtir', "Vente en l'état futur d'achèvement", "Adjudication"])]
    df = df[~df['Type local'].isin(['971', '972', '973', '974'])]
    #Suppression des 'dépendances' car elles font l'objet d'une mutation indépendantes et apportent peu au futur modèle
    df = df[df['Type local'].isin(["Maison", "Appartement", "Local industriel. commercial ou assimilé"])]
    my_variables = ['Date mutation', 'Nature mutation', 'Valeur fonciere', 'Type de voie', 'Code departement', 'Surface reelle bati', 'Type local', 'Nombre pieces principales', 'Surface terrain', 'Nombre de lots', 'Section', 'No plan', 'Adresse']
    #my_index = ['Commune', 'Voie', 'Type de voie', 'No voie', 'B/T/Q', 'Section', 'No plan', 'Date mutation', 'Code departement']
    my_index = ['Adresse', 'Section', 'No plan', 'Date mutation', 'Nature mutation']

    my_category = ['Nature mutation', 'Code departement', 'Type local', 'annee']
    my_dates = ['Date mutation']
    my_values = ['Valeur fonciere', 'Surface reelle bati', 'Nombre pieces principales', 'Surface terrain', 'Nombre de lots']
    #filtre selection variables
    df = df[my_variables]
    df['No plan'] = df['No plan'].astype('str')
    #df = df.drop_duplicates(subset=my_index) -- supprimé, doublon sur index conservé
    df = df.dropna(subset=my_values)

    df.columns = df.columns.str.replace(" ", "_")

    df = df.reset_index()

    return df

def create_db(our_host, our_dbname, our_user, our_password, auth_plugin):
    connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
    db_Info = connection.get_server_info()
    print("Connecté à Mysql: ", db_Info)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE projet3_DStest_LMJB")
    print("DATABASE CREATION SUCCESS")

def show_existing_db(our_host, our_dbname, our_user, our_password, auth_plugin):
    connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
    db_Info = connection.get_server_info()
    print("Connecté à Mysql: ", db_Info)
    cursor = connection.cursor()
    cursor.execute("SHOW DATABASES")
    db_list = []
    for db in cursor:
        db_list.append(db)
    cursor.close()
    return print(db_list)

def show_tables(our_host, our_dbname, our_user, our_password, auth_plugin):
    connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
    db_Info = connection.get_server_info()
    print("Connecté à Mysql: ", db_Info)
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    table_list = []
    for db in cursor:
        table_list.append(db)
    cursor.close()
    return print(table_list)

def create_table(tablename, our_host, our_dbname, our_user, our_password, auth_plugin):
    connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
    db_Info = connection.get_server_info()
    print("Connecté à Mysql: ", db_Info)
    cursor = connection.cursor()
    cursor.execute(f'''DROP TABLE IF EXISTS {tablename}''')
    sql =f'''CREATE TABLE {tablename}(Date_mutation DATETIME, Nature_mutation VARCHAR(255), Valeur_fonciere DECIMAL(15,2), Type_de_voie VARCHAR(255), Code_departement VARCHAR(255), Surface_reelle_bati DECIMAL(15,2), Type_local VARCHAR(255), Nombre_pieces_principales INT, Surface_terrain DECIMAL(15, 2), Nombre_de_lots INT, Section VARCHAR(255), No_plan INT, Adresse VARCHAR(255), Commune VARCHAR(255), Code_postal VARCHAR(255))'''
    cursor.execute(sql)
    cursor.close()

def insert_data(tablename, our_host, our_dbname, our_user, our_password, auth_plugin):
    connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
    db_Info = connection.get_server_info()
    print("Connecté à Mysql: ", db_Info)
    cursor = connection.cursor()
    df = pd.read_csv("./flat_file_dataframe.csv")
    sql = f'''INSERT INTO {tablename} (Date_mutation, Nature_mutation, Valeur_fonciere, Code_departement, Surface_reelle_bati, Type_local, Nombre_pieces_principales, Surface_terrain, Nombre_de_lots, Section, No_plan, Adresse, Commune, Code_postal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    counter=0
    for index, row in df.iterrows():
        cursor.execute(sql, (row.Date_mutation, row.Nature_mutation, row.Valeur_fonciere, row.Code_departement, row.Surface_reelle_bati, row.Type_local, row.Nombre_pieces_principales, row.Surface_terrain, row.Nombre_de_lots, row.Section, row.No_plan, row.Adresse, row.Commune, row.Code_postal))
        counter+=1
        print(counter)
    cursor.commit()
    print(cursor.rowcount, "lignes inserées.")
    cursor.close() 

def requesting_bdd(commune, code_postal, surface, nb_piece, typologie, our_host, our_dbname, our_user, our_password, auth_plugin):
    try:
        connection = mysql.connector.connect(host=our_host,
                                    database=our_dbname,
                                    user=our_user,
                                    password=our_password, 
                                    auth_plugin=auth_plugin)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute(f"select * from dataframe where commune = {commune} and code_postal = {code_postal} and Surface_reelle_bati = {surface} and Nombre_pieces_principales = {nb_piece} and Type_local = {typologie};")
            record = cursor.fetchall()
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

            df = pd.DataFrame(record, columns = ["date_mutation", "nature_mutation","Valeur_fonciere", "code_departement", "surface", "typologie", "nb_piece", "surface_terrain","nb_lots", "section", "noplan", "adresse", "commune", "code_postal"])
            df['valeur'] = df['valeur'].astype(float)
            df['nb_piece'] = df['nb_piece'].astype(int)
            df['surface'] = df['surface'].astype(int)
            df = df.loc[(df['surface'] == surface) & (df['nb_piece'] == nb_piece) & (df['typologie'] == typologie) & (df['commune'] == commune) & (df['code_postal'] == code_postal)]
            return df

    except Error as e:
        print("Error while connecting to MySQL", e)

def model_passing(df):
    sigma = 1
    price_estim = 1
    my_dic = {price_estim : price_estim, sigma : sigma}
    return my_dic