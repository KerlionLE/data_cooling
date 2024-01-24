from .db_connection import DBConnection
from .vertica_connection import VerticaConnection

AVAILABLE_DB_CONNECTIONS = {
    'vertica': VerticaConnection,
}
