import os
import pandas as pd
import mysql.connector

our_host = ""
our_username = ""
our_password = ""

def pull_data():
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

    #df.to_csv('temp.csv')

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

def create_db():
    mydb = mysql.connector.connect(
        host = our_host,
        user = our_username,
        password = our_password
        )

    mycursor = mydb.cursor()

    mycursor.execute("CREATE DATABASE projet3_DStest_LMJB")
    print("Successful")

def show_existing_db():
    mydb = mysql.connector.connect(
        host = our_host,
        user = our_username,
        password = our_password
    )

    mycursor = mydb.cursor()
    mycursor.execute("SHOW DATABASES")

    db_list = []
    for db in mycursor:
        db_list.append(db)
    return print(db_list)

def access_db_cursor():
    mydb = mysql.connector.connect(
        host = our_host,
        user = our_username,
        password = our_password,
        database = "projet3_DStest_LMJB"
    )
    return mydb.cursor()

def create_table():
    my_db_cursor = access_db_cursor()
    #df = pull_data()
    #my_float_col = df.select_dtypes('float').column.to_list()
    #my_int_col = df.select_dtypes('int').column.to_list()
    #my_str_col = df.select_dtypes('object').column.to_list()
    my_db_cursor.execute("CREATE TABLE dataframe (Date_mutation DATETIME, Nature_mutation VARCHAR(255), Valeur_fonciere DECIMAL(15,2), Type_de_voie VARCHAR(255), Code_departement VARCHAR(255), Surface_reelle_bati DECIMAL(15,2), Type_local VARCHAR(255), Nombre_pieces_principales INT, Surface_terrain DECIMAL(15, 2), Nombre_de_lots INT, Section VARCHAR(255), No_plan INT, Adresse VARCHAR(255))")

def insert_data():
    my_db_cursor = access_db_cursor()
    df = pull_data()
    sql = "INSERT INTO dataframe (Date_mutation, Nature_mutation, Valeur_fonciere, Type_de_voie, Code_departement, Surface_reelle_bati, Type_local, Nombre_pieces_principales, Surface_terrain, Nombre_de_lots, Section, No_plan, Adresse) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for index, row in df.iterrows():
        my_db_cursor.execute("INSERT INTO dataframe (Date_mutation, Nature_mutation, Valeur_fonciere, Type_de_voie, Code_departement, Surface_reelle_bati, Type_local, Nombre_pieces_principales, Surface_terrain, Nombre_de_lots, Section, No_plan, Adresse) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row.Date_mutation, row.Nature_mutation, row.Valeur_fonciere, row.Type_de_voie, row.Code_departement, row.Surface_reelle_bati, row.Type_local, row.Nombre_pieces_principales, row.Surface_terrain, row.Nombre_de_lots, row.Section, row.No_plan, row.Adresse)
    my_db_cursor.commit()
    print(my_db_cursor.rowcount, "lignes inserées.")
    my_db_cursor.close() 