from sqlalchemy.orm import Session
from .users import process_user_creation
from .refs import process_reference_creation
from .inventory import process_purchase

class HydrationRouter:
    @staticmethod
    def dispatch(db: Session, action: str, payload: dict):
        """Enruta la acción al procesador correspondiente."""
        
        if action == "CREAR_USUARIO":
            process_user_creation(db, payload)
            
        elif action == "CREAR_REFERENCIA":
            process_reference_creation(db, payload)
            
        elif action == "REGISTRAR_COMPRA":
            process_purchase(db, payload)
            
        # Aquí agregaremos más acciones en el futuro (VENTAS, GASTOS, etc)