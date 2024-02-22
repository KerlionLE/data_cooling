from data_cooling.connect_manager.db_connection import DBConnection
from data_cooling.connect_manager.vertica_connection import VerticaConnection

AVAILABLE_DB_CONNECTIONS = {
    'vertica': VerticaConnection,
}

AVAILABLE_DB = {
    'vertica': DBConnection,
}
