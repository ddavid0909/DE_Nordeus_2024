import os
class Configuration():
    DATABASE_HOST = 'localhost' if 'DATABASE_HOST' not in os.environ else os.environ['DATABASE_HOST']
    DATABASE_PORT = '5432' if 'DATABASE_PORT' not in os.environ else os.environ['DATABASE_PORT']

    SQLALCHEMY_DATABASE_URI = f'postgresql+psycopg2://postgres:postgres_root_password@{DATABASE_HOST}:{DATABASE_PORT}/auth_db'
