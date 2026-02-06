from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from models import Compra, Viaje
from utils import safe_int, safe_float
import traceback

def process_purchase(db: Session, payload: dict):
    try:
        args = payload.get('args', [])
        if len(args) < 2: return

        header = args[0]
        items = args[1]
        
        # 1. Asegurar Integridad Referencial
        vid = safe_int(header.get('viaje_id'))
        if vid > 0:
            exists = db.execute(text("SELECT 1 FROM viajes WHERE id=:id"), {"id": vid}).fetchone()
            if not exists:
                print(f"[HYDRATION] Auto-generando Viaje Fantasma ID {vid}")
                new_viaje = Viaje(id=vid, nombre=f"Ref-Sync-{vid}", peso_kg=0)
                db.add(new_viaje)
                try: db.flush() 
                except: db.rollback()

        # 2. Insertar Items
        folio_global = str(payload.get('folio', '') or '')
        
        for item in items:
            item_uuid = item.get('uuid')
            if item_uuid:
                dupe = db.query(Compra).filter(text("uuid = :u")).params(u=item_uuid).first()
                if dupe: continue

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
        print(f"[HYDRATION ERROR] Compra: {traceback.format_exc()}")