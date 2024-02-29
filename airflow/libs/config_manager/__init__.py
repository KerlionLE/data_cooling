from .conf_manager import ConfigManager
from .json_conf_manager import JsonConfManager

AVAILABLE_FORMAT_MANAGER = {
    'json': JsonConfManager,
}

AVAILABLE_FORMAT = {
    'json': ConfigManager,
}
