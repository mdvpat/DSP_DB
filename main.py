from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List
from pydantic import BaseModel
import pandas as pd
import random
import os
import fonctions as func
import mysql.connector
from mysql.connector import Error

### Import BD et creation de deux listes pour USE et SUBJECT
###
###
our_host = "44.204.92.180"
our_dbname = "Projet3_DStest_LMJB"
our_user = 'root'
our_password = os.getenv('MYSQL_ROOT_PASSWORD')
auth_plugin = 'mysql_native_password'


### Déclaration objet API et nommage
###
###
api = FastAPI(
    title='Datascientest Projet 3 Lurie, Marin, JB',
    description="API projet final Data-engineer Datascientest Iurie, Marin, JB",
    version="1.0.1",
    openapi_tags=[
      {
        'name': 'home',
        'description': 'Home page'
      },
      {
        'name': 'param',
        'description': 'Obtenir les paramêtres'
      },
      {
        'name': 'admin',
        'description': "Section administration"
      }
    ]
)

#############################################################################################
### Authentification avec OAuth2
#############################################################################################
user_database = {
  "admin": "Ds2022+++",
  }

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def authenticate_user(username, password):
  if password == user_database[username]:
    return True
  else : 
    return False

@api.post("/token", name='Création du formulaire de login', tags=['admin'])
def login(form_data: OAuth2PasswordRequestForm = Depends()):
  username = form_data.username
  password = form_data.password

  if authenticate_user(username, password):
    return {"access_token": username, "token_type": "bearer"}
  else :
    raise HTTPException(status_code=400, detail="Incorrect username or password")

#############################################################################################
### Création de la classe Parameter pour requeter la BDD et spécifier le corps de la requête
#############################################################################################
class Parameter(BaseModel):
    """
    Paramêtres du modèle
    """
    surface: int
    typologie: str
    commune: str

#############################################################################################
### API
#############################################################################################
@api.get("/", name="Home page de l'API", tags=['home'])
def home(): #token: str = Depends(oauth2_scheme)
  return {} #{"token": token}

@api.post('/param', name = "Obtenir les paramêtres'", tags=['param'])
async def post_param(param: Parameter): #token: str = Depends(oauth2_scheme)
  """ 
  Obtention des paramêtres depuis formulaire app
  """
  df_bdd_return = func.requesting_bdd(param.commune, param.typologie, our_host, our_dbname, our_user, our_password, auth_plugin)
  result = func.model_passing(df_bdd_return, param.surface)
  df_bdd = df_bdd_return.to_dict('index')
  return {"transactions": df_bdd, "result": result}


#############################################################################################
###Adding New User
#############################################################################################
class NewUser(BaseModel):
  username: str
  password: str

@api.post("/add_user", name = "Ajout d'un user avec compte admin", tags=['admin'])
async def add_user(new_user: NewUser, token: str = Depends(oauth2_scheme)):
  """ 
  Ajout d'un user dans la base de donnée.<br>
  Seul un compte administrateur peut réaliser cette opération<br>
  Le corps doit contenir un username et un password, format str.
  """
  if token == "admin" :
    user_database[new_user.username] = new_user.password
    return "User ajouté avec succès", user_database
  else :
    raise HTTPException(status_code=400, detail="Vous n'avez pas les droits pour effectuer cette opération.")
