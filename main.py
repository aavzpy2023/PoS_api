from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql import text
from datetime import datetime
import json
import os

# --- 1. CONFIGURACIÓN ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL: exit(1)

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if "channel_binding" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.split("?")[0] + "?sslmode=require"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- 2. MODELOS ESPEJO (NEON SCHEMA) ---

class SystemAudit(Base):
    __tablename__ = "system_audit"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime(timezone=True))
    action_type = Column(String, index=True)
    payload_json = Column(JSONB)
    user = Column(String, name="user")
    app_version = Column(String)
    hash = Column(String)
    global_event_id = Column(UUID(as_uuid=True), unique=True)
    sync_status = Column(Integer, default=1)


class Viaje(Base):
    __tablename__ = "viajes"
    id = Column(Integer, primary_key=True)  # Mantenemos ID original
    nombre = Column(String, unique=True)
    peso_kg = Column(Numeric(10, 2))
    activo = Column(Boolean, default=True)


class Compra(Base):
    __tablename__ = "compras"
    id = Column(Integer, primary_key=True)  # Mantenemos ID original si viene, o autoincrement
    uuid = Column(UUID(as_uuid=True), unique=True, nullable=True)
    producto = Column(String)
    precio_venta = Column(Numeric(12, 2))
    viaje_id = Column(Integer)  # FK lógica
    cantidad = Column(Numeric(12, 4))
    costo_unit_mxn = Column(Numeric(12, 2))
    tasa_mxn_usd = Column(Numeric(10, 4))
    tasa_cuc_usd = Column(Numeric(10, 4))
    liquidado = Column(Boolean)
    monto_pagado = Column(Numeric(12, 2))
    categoria = Column(String)
    unidad_medida = Column(String)
    costo_unit_cuc_snapshot = Column(Numeric(12, 2))
    es_inversion = Column(Boolean)
    folio = Column(String)
    fecha_creacion = Column(DateTime(timezone=True), server_default=text("now()"))


# Crear tablas si no existen (Auto-migración simple)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Nexuite Sync API v2.1 (Hydration)")


# --- 3. LOGICA DE HIDRATACIÓN (EL CEREBRO) ---
def hydrate_operational_tables(db: Session, action: str, payload: Dict):
    """
    Desempaqueta el JSON de auditoría y llena las tablas reales.
    """
    if action == "REGISTRAR_COMPRA":
        # Estructura payload: {'args': [header_dict, [item1, item2...]]}
        try:
            args = payload.get('args', [])
            if len(args) < 2: return

            header = args[0]
            items = args[1]

            # 1. Asegurar Referencia (Viaje)
            vid = header.get('viaje_id')
            if vid:
                # Verificar si existe, si no, crear placeholder para integridad FK
                viaje = db.query(Viaje).filter(Viaje.id == vid).first()
                if not viaje:
                    new_viaje = Viaje(id=vid, nombre=f"Ref-Sync-{vid}", peso_kg=0)
                    db.add(new_viaje)
                    db.flush()  # Commit parcial para que la FK funcione

            # 2. Insertar Items de Compra
            folio_global = str(payload.get('folio', ''))  # A veces el folio está en el payload raíz o en items

            for item in items:
                # Evitar duplicados por UUID si existe
                item_uuid = item.get('uuid')
                if item_uuid:
                    exists = db.query(Compra).filter(text("uuid = :u")).params(u=item_uuid).first()
                    if exists: continue

                # Mapeo de campos
                new_compra = Compra(
                    uuid=item_uuid,
                    producto=item.get('producto'),
                    precio_venta=item.get('precio_venta', 0),
                    viaje_id=vid,
                    cantidad=item.get('cantidad', 0),
                    costo_unit_mxn=item.get('costo_mxn', 0),
                    tasa_mxn_usd=item.get('tasa_mxn_snap', 1),
                    tasa_cuc_usd=item.get('tasa_cuc_snap', 1),
                    liquidado=header.get('liquidado_global', True),
                    monto_pagado=0,  # Simplificación
                    categoria=item.get('categoria', 'PRODUCTO'),
                    unidad_medida=item.get('unidad', 'uds'),
                    costo_unit_cuc_snapshot=item.get('costo_cuc_visual', 0),
                    es_inversion=header.get('es_inversion', False),
                    folio=item.get('folio') or folio_global
                )
                db.add(new_compra)

        except Exception as e:
            print(f"[HYDRATION ERROR] {action}: {e}")
            # No lanzamos error para no abortar el sync del Audit Log, solo logueamos
            pass


# --- 4. ENDPOINTS ---
API_KEY = os.getenv("API_KEY_EXPECTED")


def verify_key(api_key: str = Header(None)):
    if api_key != API_KEY: raise HTTPException(403, "Forbidden")


class AuditEvent(BaseModel):
    timestamp: str
    action_type: str
    payload_json: str
    user: str
    app_version: str
    hash: str
    global_event_id: str


@app.post("/sync/push", dependencies=[Depends(verify_key)])
def sync(events: List[AuditEvent]):
    db = SessionLocal()
    inserted = 0
    try:
        for ev in events:
            # 1. Idempotencia Audit
            exists = db.query(SystemAudit).filter(text("global_event_id = :g")).params(g=ev.global_event_id).first()
            if exists: continue

            # 2. Parsear JSON
            try:
                p_dict = json.loads(ev.payload_json)
            except:
                p_dict = {}

            # 3. Guardar en Auditoría (Receipt)
            audit = SystemAudit(
                timestamp=datetime.strptime(ev.timestamp, "%Y-%m-%d %H:%M:%S"),
                action_type=ev.action_type,
                payload_json=p_dict,
                user=ev.user,
                app_version=ev.app_version,
                hash=ev.hash,
                global_event_id=ev.global_event_id
            )
            db.add(audit)

            # 4. HIDRATAR TABLAS OPERATIVAS (The Magic)
            hydrate_operational_tables(db, ev.action_type, p_dict)

            inserted += 1

        db.commit()
        return {"status": "success", "inserted": inserted}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, str(e))
    finally:
        db.close()