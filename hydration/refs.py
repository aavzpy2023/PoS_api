from sqlalchemy.orm import Session
from models import Viaje
from utils import safe_int, safe_float

def process_reference_creation(db: Session, payload: dict):
    try:
        r_id = safe_int(payload.get('id'))
        r_nom = payload.get('nombre')
        r_peso = safe_float(payload.get('peso'))

        if r_id > 0 and r_nom:
            exists = db.query(Viaje).filter(Viaje.id == r_id).first()
            if not exists:
                print(f"[HYDRATION] Creando Ref: {r_nom} (ID: {r_id})")
                new_ref = Viaje(id=r_id, nombre=r_nom, peso_kg=r_peso, activo=True)
                db.add(new_ref)
    except Exception as e:
        print(f"[HYDRATION ERROR] Ref: {e}")