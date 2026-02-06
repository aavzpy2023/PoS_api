from sqlalchemy import Column, Integer, String, Boolean, Numeric, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from database import Base

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

class Usuario(Base):
    __tablename__ = "usuarios"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password_hash = Column(String)
    rol = Column(String, default="admin")
    telefono = Column(String, default="")

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