from sqlalchemy.orm import Session
from models import Usuario

def process_user_creation(db: Session, payload: dict):
    try:
        u_name = payload.get('username')
        u_pass = payload.get('password_hash')
        
        if u_name and u_pass:
            exists = db.query(Usuario).filter(Usuario.username == u_name).first()
            if not exists:
                print(f"[HYDRATION] Creando usuario: {u_name}")
                new_user = Usuario(username=u_name, password_hash=u_pass, rol="admin")
                db.add(new_user)
    except Exception as e:
        print(f"[HYDRATION ERROR] User: {e}")