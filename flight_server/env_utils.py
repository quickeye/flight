import os
from typing import Optional, Any

def get_env_var(var_name: str, default: Any = None) -> Any:
    """Get environment variable with fallback to default value."""
    return os.getenv(var_name, default)

def get_int_env_var(var_name: str, default: int) -> int:
    """Get environment variable as integer with fallback to default value."""
    try:
        return int(get_env_var(var_name, str(default)))
    except (ValueError, TypeError):
        return default

def get_bool_env_var(var_name: str, default: bool) -> bool:
    """Get environment variable as boolean with fallback to default value."""
    value = get_env_var(var_name, str(default).lower())
    if isinstance(value, str):
        return value.lower() in ['true', '1', 't', 'y', 'yes']
    return bool(value)

def get_list_env_var(var_name: str, default: list, delimiter: str = ',') -> list:
    """Get environment variable as list with fallback to default value."""
    value = get_env_var(var_name, ','.join(map(str, default)))
    if isinstance(value, str):
        return [item.strip() for item in value.split(delimiter)]
    return default

def get_dict_env_var(var_name: str, default: dict, delimiter: str = ',') -> dict:
    """Get environment variable as dictionary with fallback to default value."""
    value = get_env_var(var_name, None)
    if value:
        try:
            return dict(item.split('=') for item in value.split(delimiter))
        except ValueError:
            return default
    return default
