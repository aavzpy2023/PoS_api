from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Any
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime
from sqlalchemy.dialects.postgresql import JSONB, UUID  # Tipos nativos de Neon
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text
from datetime import datetime
import json
import os

# --- 1. CONFIGURACIÓN DE CONEXIÓN ---
# Railway inyecta DATABASE_URL automáticamente.
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("FATAL: DATABASE_URL no encontrada.")
    # NO USAR FALLBACK A SQLITE para obligar a ver el error si falla la conexión
    exit(1)

# Parches para compatibilidad con Neon/SQLAlchemy
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Limpieza de parámetros SSL que a veces confunden a Python
if "channel_binding" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.split("?")[0] + "?sslmode=require"

print(f" Connecting to: {DATABASE_URL.split('@')[-1]}")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- 2. MODELO ORM (MAPEO EXACTO A TU SQL) ---
class SystemAudit(Base):
    __tablename__ = "system_audit"

    # Mapeo exacto a tu CREATE TABLE system_audit
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=text("now()"))
    action_type = Column(String, nullable=False, index=True)

    # CRÍTICO: Neon espera JSONB real, no string.
    payload_json = Column(JSONB, nullable=False)

    user = Column(String, name="user")  # "user" es palabra reservada, mapeamos explícitamente
    app_version = Column(String)
    hash = Column(String, nullable=False)
    global_event_id = Column(UUID(as_uuid=True), unique=True, nullable=True)  # UUID Nativo
    sync_status = Column(Integer, default=1)


app = FastAPI(title="Nexuite Sync API v2")


# --- 3. DTO (LO QUE ENVÍA LA APP) ---
class AuditEvent(BaseModel):
    timestamp: str
    action_type: str
    payload_json: str  # La app envía esto como STRING serializado
    user: str
    app_version: str
    hash: str
    global_event_id: str


# --- 4. SEGURIDAD ---
API_KEY_EXPECTED = os.getenv("API_KEY_EXPECTED")


def verify_api_key(api_key: str = Header(None)):
    if not API_KEY_EXPECTED or api_key != API_KEY_EXPECTED:
        raise HTTPException(status_code=403, detail="API Key Inválida")
    return api_key


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- 5. ENDPOINTS ---

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        # Prueba real de conexión a Neon
        result = db.execute(text("SELECT 1")).scalar()
        return {
            "status": "online",
            "database": "Neon PostgreSQL",
            "connection": "OK" if result == 1 else "FAIL"
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/sync/push", dependencies=[Depends(verify_api_key)])
def receive_events(events: List[AuditEvent], db: Session = Depends(get_db)):
    inserted_count = 0
    errors = []

    for ev in events:
        try:
            # 1. Idempotencia: Verificar si el UUID ya existe en Neon
            # Usamos cast a string para comparar UUIDs de forma segura
            exists = db.query(SystemAudit).filter(
                text("global_event_id = :gid")
            ).params(gid=ev.global_event_id).first()

            if exists:
                continue  # Ya existe, saltar silenciosamente (éxito)

            # 2. Conversión de Tipos (El paso CRÍTICO)
            # Convertir el string JSON de la app a Diccionario Python
            # para que SQLAlchemy lo guarde como JSONB en Postgres
            try:
                json_data = json.loads(ev.payload_json)
            except:
                # Si falla el parseo, guardarlo como un dict simple con error
                json_data = {"raw_error": ev.payload_json}

            # Convertir Timestamp string a objeto DateTime
            try:
                # La app envía '2026-02-04 20:48:21'
                ts_obj = datetime.strptime(ev.timestamp, "%Y-%m-%d %H:%M:%S")
            except:
                ts_obj = datetime.now()

            # 3. Crear registro
            new_audit = SystemAudit(
                timestamp=ts_obj,
                action_type=ev.action_type,
                payload_json=json_data,  # Aquí pasamos el DICT, no el string
                user=ev.user,
                app_version=ev.app_version,
                hash=ev.hash,
                global_event_id=ev.global_event_id,  # SQLAlchemy maneja la conversión a UUID
                sync_status=1
            )

            db.add(new_audit)
            inserted_count += 1

        except Exception as e:
            print(f"Error procesando evento {ev.action_type}: {e}")
            errors.append(str(e))
            continue

    try:
        db.commit()
        return {
            "status": "success",
            "received": len(events),
            "inserted": inserted_count,
            "errors": errors if errors else None
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error en commit a Neon: {str(e)}")