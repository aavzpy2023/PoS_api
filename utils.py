def safe_float(val, default=0.0):
    try: return float(val)
    except: return default

def safe_int(val, default=0):
    try: return int(val)
    except: return default
    