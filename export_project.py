import os
from datetime import datetime

def export_api_context():
    output_file = "full_api_content_export.txt"
    # Archivos y carpetas a ignorar
    ignore_list = ['__pycache__', '.git', '.env', 'venv', 'full_api_content_export.txt', 'export_api.py']
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"--- Exportado: {datetime.now()} ---\n\n")
        
        for root, dirs, files in os.walk("."):
            # Filtrar carpetas ignoradas
            dirs[:] = [d for d in dirs if d not in ignore_list]
            
            for file in files:
                if file.endswith(('.py', '.txt', '.yaml', '.yml')) and file not in ignore_list:
                    rel_path = os.path.relpath(os.path.join(root, file), ".")
                    f.write(f"\n// --- {rel_path} ---\n\n")
                    try:
                        with open(os.path.join(root, file), "r", encoding="utf-8") as content:
                            f.write(content.read())
                    except Exception as e:
                        f.write(f"Error leyendo archivo: {e}")
                    f.write("\n")

    print(f"✅ Contexto de API exportado a: {output_file}")

if __name__ == "__main__":
    export_api_context()