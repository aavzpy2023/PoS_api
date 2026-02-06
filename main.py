import os
import json
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

# Imports Modulares
from database import engine, SessionLocal, get_db, Base
from models import SystemAudit
from hydration.router import HydrationRouter

# Auto-migración al inicio
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Nexuite Sync API v3.0 (Modular)")

# --- SECURITY ---
API_KEY_EXPECTED = os.getenv("API_KEY_EXPECTED")

def verify_api_key(api_key: str = Header(None)):
    if not API_KEY_EXPECTED or api_key != API_KEY_EXPECTED:
        raise HTTPException(status_code=403, detail="Forbidden")
    return api_key

# --- DTO ---
class AuditEvent(BaseModel):
    timestamp: str
    action_type: str
    payload_json: str
    user: str
    app_version: str
    hash: str
    global_event_id: str

# --- ENDPOINTS ---
@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "online", "db": "connected"}
    except Exception as e:
        return {"status": "error", "db": str(e)}

@app.post("/sync/push", dependencies=[Depends(verify_api_key)])
def sync_events(events: List[AuditEvent], db: Session = Depends(get_db)):
    inserted_count = 0
    try:
        for ev in events:
            # 1. Idempotencia
            exists = db.query(SystemAudit).filter(text("global_event_id = :g")).params(g=ev.global_event_id).first()
            if exists: continue

            try:
                p_dict = json.loads(ev.payload_json)
            except:
                p_dict = {}

            # 2. Guardar Evidencia (Audit Log)
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
            
            # 3. Hidratación Modular
            HydrationRouter.dispatch(db, ev.action_type, p_dict)
            
            inserted_count += 1
        
        db.commit()
        return {"status": "success", "inserted": inserted_count}
        
    except Exception as e:
        db.rollback()
        print(f"[SYNC FATAL] {e}")
        raise HTTPException(500, str(e))