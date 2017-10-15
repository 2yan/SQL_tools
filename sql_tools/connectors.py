def get_postgresql():
    try:
        import psycopg2
    except ImportError as e:
        print('Install Pypyodbc For this to work')
        raise e
    
    func = psycopg2.connect
    func.name = 'postgresql'
    return func

def get_microsoft_sql():
    try:
        import pypyodbc
    except ImportError as e:
        print('Install Pypyodbc For this to work')
        raise e
        
    func = pypyodbc.connect
    func.name = 'microsoft_sql'
    return func
