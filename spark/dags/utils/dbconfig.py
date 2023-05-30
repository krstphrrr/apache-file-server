from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool

def config(filename='src/utils/database.ini', section='maindev'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db

class db:
    def __init__(self, keyword = 'newtall'):
        if keyword == None:
            self.params = config()
            self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
        else:
            if "maindev" in keyword:
                self.params = config(section='maindev')
                self.params['options'] = "-c search_path=public_dev"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()

            elif "gis" in keyword:
                self.params = config(section='postgresql')
                self.params['options'] = "-c search_path=gis"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()

            elif "newtall" in keyword:
                self.params = config(section='newtall')
                self.params['options'] = "-c search_path=public_test"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()

            else:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=public"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()
