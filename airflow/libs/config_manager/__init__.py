from .conf_manager import ConfigManager
from .json_conf_manager import JsonConfManager
from .datacatalog_conf_manager import DataCatalogConfManager

AVAILABLE_FORMAT_MANAGER = {
    'json': JsonConfManager,
    'datacatalog': DataCatalogConfManager
}
