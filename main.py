from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from typing import List
import sqlalchemy
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
import os

# --- DATABASE CONFIG ---
# Railway inyectará esta variable automáticamente si la configuramos
DATABASE_URL = os.getenv("DATABASE_URL")

engine = sqlalchemy.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI(title="Nexuite Sync API")

# --- SECURITY ---
API_KEY_EXPECTED = os.getenv("API_KEY_EXPECTED")


def verify_api_key(api_key: str = Header(None)):
    if api_key != API_KEY_EXPECTED:
        raise HTTPException(status_code=403, detail="Acceso Denegado")
    return api_key


# --- DTOs ---
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
def health_check():
    return {"status": "online", "db": "connected" if DATABASE_URL else "missing_config"}


@app.post("/sync/push", dependencies=[Depends(verify_api_key)])
def receive_events(events: List[AuditEvent], db: Session = Depends(lambda: SessionLocal())):
    try:
        count = 0
        for ev in events:
            # Idempotencia: Si ya existe el UUID global, lo ignoramos
            exists = db.execute(
                sqlalchemy.text("SELECT 1 FROM system_audit WHERE global_event_id = :gid"),
                {"gid": ev.global_event_id}
            ).fetchone()

            if not exists:
                db.execute(
                    sqlalchemy.text("""
                        INSERT INTO system_audit 
                        (timestamp, action_type, payload_json, "user", app_version, hash, global_event_id, sync_status)
                        VALUES (:ts, :at, :pj, :usr, :ver, :h, :gid, 1)
                    """),
                    {
                        "ts": ev.timestamp, "at": ev.action_type, "pj": ev.payload_json,
                        "usr": ev.user, "ver": ev.app_version, "h": ev.hash, "gid": ev.global_event_id
                    }
                )
                count += 1
        db.commit()
        return {"status": "success", "inserted": count}
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()