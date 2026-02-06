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
import traceback # Para ver el error real

# --- 1. CONFIGURACIÓN ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL: 
    print("FATAL: DATABASE_URL missing")
    exit(1)

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
    id = Column(Integer, primary_key=True)
    nombre = Column(String, unique=True)
    peso_kg = Column(Numeric(10, 2))
    activo = Column(Boolean, default=True)

class Compra(Base):
    __tablename__ = "compras"
    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(UUID(as_uuid=True), unique=True, nullable=True)
    producto = Column(String)
    precio_venta = Column(Numeric(12, 2))
    viaje_id = Column(Integer)
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

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Nexuite Sync API v2.2 (Debug)")

# --- 3. HELPER DE LIMPIEZA ---
def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def safe_int(val, default=0):
    try:
        return int(val)
    except:
        return default

# --- 4. LOGICA DE HIDRATACIÓN ---
def hydrate_operational_tables(db: Session, action: str, payload: Dict):
    if action == "REGISTRAR_COMPRA":
        try:
            # Estructura esperada: {'args': [header, [items]]}
            args = payload.get('args', [])
            if len(args) < 2: 
                print(f"[HYDRATION SKIP] Payload incompleto: {payload.keys()}")
                return

            header = args[0]
            items = args[1]
            
            # 1. Referencia
            vid = safe_int(header.get('viaje_id'))
            if vid > 0:
                # Verificar si existe usando SQL directo para evitar cache stale
                exists = db.execute(text("SELECT 1 FROM viajes WHERE id=:id"), {"id": vid}).fetchone()
                if not exists:
                    # Crear Viaje Placeholder
                    print(f"[HYDRATION INFO] Creando Viaje ID {vid}")
                    new_viaje = Viaje(id=vid, nombre=f"Ref-Sync-{vid}", peso_kg=0)
                    db.add(new_viaje)
                    try:
                        db.flush() 
                    except Exception as e:
                        print(f"[HYDRATION WARN] Error creando viaje (posible duplicado ignorado): {e}")
                        db.rollback()

            # 2. Items
            folio_global = str(payload.get('folio', '') or '')
            
            for item in items:
                # UUID Check
                item_uuid = item.get('uuid')
                if not item_uuid:
                    # Si no trae UUID (logs viejos), generamos uno determinista o saltamos
                    # Por ahora saltamos validación estricta de UUID
                    item_uuid = None
                
                if item_uuid:
                    # Chequeo de duplicados
                    dupe = db.query(Compra).filter(text("uuid = :u")).params(u=item_uuid).first()
                    if dupe: 
                        print(f"[HYDRATION SKIP] Item {item.get('producto')} ya existe (UUID {item_uuid})")
                        continue

                # Inserción
                print(f"[HYDRATION INSERT] Insertando: {item.get('producto')}")
                new_compra = Compra(
                    uuid=item_uuid,
                    producto=str(item.get('producto', 'Unknown')),
                    precio_venta=safe_float(item.get('precio_venta')),
                    viaje_id=vid if vid > 0 else None,
                    cantidad=safe_float(item.get('cantidad')),
                    costo_unit_mxn=safe_float(item.get('costo_mxn')),
                    tasa_mxn_usd=safe_float(item.get('tasa_mxn_snap', 1)),
                    tasa_cuc_usd=safe_float(item.get('tasa_cuc_snap', 1)),
                    liquidado=bool(header.get('liquidado_global', True)),
                    monto_pagado=0, 
                    categoria=str(item.get('categoria', 'PRODUCTO')),
                    unidad_medida=str(item.get('unidad', 'uds')),
                    costo_unit_cuc_snapshot=safe_float(item.get('costo_cuc_visual')),
                    es_inversion=bool(header.get('es_inversion', False)),
                    folio=str(item.get('folio') or folio_global)
                )
                db.add(new_compra)
                
        except Exception as e:
            # AHORA SÍ IMPRIMIMOS EL ERROR REAL
            print(f"!!! [HYDRATION CRITICAL ERROR] !!!")
            print(traceback.format_exc())
            # No hacemos raise para no tumbar el sync del Audit, pero quedará en logs de Railway

# --- 5. ENDPOINTS ---
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
            # Idempotencia
            exists = db.query(SystemAudit).filter(text("global_event_id = :g")).params(g=ev.global_event_id).first()
            if exists: continue

            try:
                p_dict = json.loads(ev.payload_json)
            except:
                p_dict = {}

            # Guardar Audit
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
            
            # Intentar Hidratar (Con Logs)
            hydrate_operational_tables(db, ev.action_type, p_dict)
            
            inserted += 1
        
        db.commit()
        return {"status": "success", "inserted": inserted}
    except Exception as e:
        db.rollback()
        print(f"[SYNC ERROR] {e}")
        raise HTTPException(500, str(e))
    finally:
        db.close()