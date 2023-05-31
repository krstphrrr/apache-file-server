import utils.table_utils as tutils

def create_command(tablename):
    schema = tutils.todict(tablename)
    init = 'CREATE TABLE IF NOT EXISTS'
    fields = ''
    verb = f'{init} "{tablename}" ( rid SERIAL PRIMARY KEY, '
    for k,v in schema.items():
        if k!=list(schema.keys())[-1]:
            fields+=f'"{k}" {v}, '
        else:
            fields+=f'"{k}" {v}'

    verb+= f'{fields});'
    if "geoIndicators" in tablename or "dataHeader" in tablename:
      verb = verb.replace('"PrimaryKey" TEXT,','"PrimaryKey" TEXT UNIQUE,')

    return verb