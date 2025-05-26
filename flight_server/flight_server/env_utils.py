"""
Utility functions for handling environment variables
"""

import os
from typing import Optional, Any

def get_env_var(name: str, default: Optional[Any] = None) -> Any:
    """
    Get an environment variable with a fallback to default value
    
    Args:
        name: Name of the environment variable
        default: Default value if the environment variable is not set
        
    Returns:
        The value of the environment variable or the default value
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value

def get_env_var_int(name: str, default: Optional[int] = None) -> Optional[int]:
    """
    Get an environment variable as an integer
    
    Args:
        name: Name of the environment variable
        default: Default value if the environment variable is not set
        
    Returns:
        The integer value of the environment variable or the default value
    """
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default

def get_env_var_bool(name: str, default: Optional[bool] = None) -> Optional[bool]:
    """
    Get an environment variable as a boolean
    
    Args:
        name: Name of the environment variable
        default: Default value if the environment variable is not set
        
    Returns:
        The boolean value of the environment variable or the default value
    """
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ['true', '1', 't', 'y', 'yes']
