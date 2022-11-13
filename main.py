from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List
from pydantic import BaseModel
import pandas as pd

### Import MODEL

###

### Déclaration objet API et nommage
###
###
api = FastAPI(
    title='Datascientest Projet 3 Lurie, Marin, JB',
    description="API projet final Data-engineer Datascientest Lurie, Marin, JB",
    version="1.0.1",
    openapi_tags=[
      {
        'name': 'home',
        'description': 'Home page'
      },
      {
        'name': 'readme',
        'description': 'Obtenir la liste des options disponibles pour requeter le modèle'
      },
      {
        'name': 'prix_estim',
        'description': 'Obtenir le questionnaire'
      },
      {
        'name': 'admin',
        'description': "Section administration"
      }
    ]
)

### Authentification avec OAuth2
###
###
user_database = {
    "Lurie": "Lurie",
    "Marin": "Marin",
    "Jb": "Jb",
    "admin": "admin" 
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

@api.get("/", name="Home page de l'API", tags=['home'])
def home(token: str = Depends(oauth2_scheme)):
  return {"token": token}

###Fonctions outils
###
###

### Création de la classe QUESTION pour requeter la BDD et spécifier le corps de la requête
###
###
class Request(BaseModel):
    """
    Le choix permet d'entrer un "Use" et une liste de "subject"
    """
    critere1: List[str]
    critere2: List[str]
    critere3: str

### Fonction renvoyant la liste des use
###
###
@api.get('/readme', name = "", tags=['readme'])
async def get_info(token: str = Depends(oauth2_scheme)):
  """ 
  Obtenir le manuel de l'API
  """
  return {}

### Fonction renvoyant la liste des subject
###
###
@api.get('/prix_estim', name = "Obtenir l'estimation d'un prix et le sigma", tags=['prix_estim'])
async def get_prix_estim(token: str = Depends(oauth2_scheme)):
    """ 
    Obtenir l'estimation d'un prix et les sigma'
    """
    return {} 

###Adding New User
###
###
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


