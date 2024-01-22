from .db_connection import DBConnection
from .vertica_connection import VerticaConnection
from .hdfs_connection import HdfsConnection

AVAILABLE_DB_CONNECTIONS = {
    'vertica': VerticaConnection,
    'hdfs': HdfsConnection,
}
