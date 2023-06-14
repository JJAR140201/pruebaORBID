from fastapi import FastAPI, BackgroundTasks
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

# Configuración de la base de datos
DB_HOST = "db.g97.io"
DB_PORT = 5432
DB_USER = "developer"
DB_PASS = "qS*7Pjs3v0kw"
DB_NAME = "data_analyst"
DB_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear la conexión a la base de datos
engine = create_engine(DB_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Definir el modelo de la tabla api_calls
class APICall(Base):
    __tablename__ = "api_calls"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime)
    request_url = Column(String)
    response_code = Column(Integer)

    def __init__(self, timestamp, request_url, response_code):
        self.timestamp = timestamp
        self.request_url = request_url
        self.response_code = response_code

# Crear la aplicación FastAPI
app = FastAPI()

@app.post("/contacts/")
def create_contact(name: str, email: str, background_tasks: BackgroundTasks):
    # Crear el contacto en HubSpot
    hubspot_url = "https://api.hubapi.com/crm/v3/objects/contacts"
    access_token = "pat-na1-bfa3f0c0-426b-4f0e-b514-89b20832c96a"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    data = {
        "properties": {
            "firstname": name,
            "email": email
        }
    }
    hubspot_response = requests.post(hubspot_url, headers=headers, json=data)

    # Registrar la llamada a la API en la base de datos
    db = SessionLocal()
    api_call = APICall(timestamp=datetime.now(), request_url=hubspot_url, response_code=hubspot_response.status_code)
    db.add(api_call)
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise e
    finally:
        db.close()

    # Sincronizar el contacto con ClickUp en segundo plano
    if hubspot_response.status_code == 201:
        background_tasks.add_task(sync_contact_with_clickup, name, email)

    return {"hubspot_status_code": hubspot_response.status_code}

def sync_contact_with_clickup(name: str, email: str):
    # Validar si el contacto ya existe en ClickUp
    clickup_url = f"https://api.clickup.com/api/v2/list/900200532843/task?name={name}"
    token = "pk_3182376_Q233NZDZ8AVULEGGCHLKG2HFXWD6MJLC"
    headers = {"Authorization": token}
    clickup_response = requests.get(clickup_url, headers=headers)

    if clickup_response.status_code == 200 and len(clickup_response.json()["tasks"]) == 0:
        # Crear la tarea en ClickUp
        clickup_url = "https://api.clickup.com/api/v2/task"
        data = {"name": name, "email": email, "list_id": "900200532843"}
        clickup_response = requests.post(clickup_url, headers=headers, json=data)

    # Registrar la llamada a la API en la base de datos
    db = SessionLocal()
    api_call = APICall(timestamp=datetime.now(), request_url=clickup_url, response_code=clickup_response.status_code)
    db.add(api_call)
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise e
    finally:
        db.close()

def update_contact(contact_id: str, name: str, email: str):
    # Actualizar el contacto en HubSpot
    hubspot_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    access_token = "pat-na1-bfa3f0c0-426b-4f0e-b514-89b20832c96a"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    data = {
        "properties": {
            "firstname": name,
            "email": email
        }
    }
    hubspot_response = requests.patch(hubspot_url, headers=headers, json=data)

    # Registrar la llamada a la API en la base de datos
    db = SessionLocal()
    api_call = APICall(timestamp=datetime.now(), request_url=hubspot_url, response_code=hubspot_response.status_code)
    db.add(api_call)
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise e
    finally:
        db.close()

    return {"hubspot_status_code": hubspot_response.status_code}

@app.delete("/contacts/{contact_id}")
def delete_contact(contact_id: str):
    # Eliminar el contacto en HubSpot
    hubspot_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    access_token = "pat-na1-bfa3f0c0-426b-4f0e-b514-89b20832c96a"
    headers = {"Authorization": f"Bearer {access_token}"}
    hubspot_response = requests.delete(hubspot_url, headers=headers)

    # Registrar la llamada a la API en la base de datos
    db = SessionLocal()
    api_call = APICall(timestamp=datetime.now(), request_url=hubspot_url, response_code=hubspot_response.status_code)
    db.add(api_call)
    try:
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise e
    finally:
        db.close()

    return {"hubspot_status_code": hubspot_response.status_code}


