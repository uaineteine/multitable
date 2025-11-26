"""
This module defines constant parameters used across the application.

It includes functions to detect whether JAVA_HOME is set and to configure it programmatically.

Version:
    1.0
"""

import os
from .multitable import MultiTable
from .schema_val import SchemaValidator
from .frame_check import FrameTypeVerifier

# CONSTANT PARAMETERS FOR MODULE

module_version = "1.0"
meta_version = "1.0"
"""str: The version number of this module.
Used to track compatibility and changes across releases.
"""

def expected_meta_version(this_version:str) -> bool:
    """
    Returns a bool if the input version matches the meta version of the library framework it was run on. Will print user warnings if the value mismatches
    """
    cnd_match = meta_version == this_version

    if (not cnd_match):
        print(f"DAG Code expects meta version of: {meta_version}")
    
    return cnd_match

def detect_if_hadoop_home_set() -> bool:
    """
    Checks whether the HADOOP_HOME environment variable is set and points to a valid directory.

    Returns:
        bool: True if HADOOP_HOME is set and the path exists, False otherwise.
    """
    home = os.environ.get("HADOOP_HOME")
    if home and os.path.exists(home):
        return True
    return False

def setup_hadoop_home(hadoop_home_path: str):
    """
    Sets the HADOOP_HOME environment variable to the specified path.

    Args:
        hadoop_home_path (str): The path to the HADOOP installation directory.

    Raises:
        ValueError: If the provided path does not exist.
    """
    if not os.path.exists(hadoop_home_path):
        raise ValueError(f"Provided HADOOP_HOME path does not exist: {hadoop_home_path}")
    os.environ["HADOOP_HOME"] = hadoop_home_path

def list_all_environment_variables() -> dict:
    """
    Lists all environment variables currently set in the system.

    Returns:
        dict: A dictionary containing all environment variables as key-value pairs.
    """
    return dict(os.environ)

def print_all_environment_variables():
    """
    Prints all environment variables currently set in the system in a formatted way.
    """
    env_vars = os.environ
    print("Environment Variables:")
    print("-" * 120)
    for key, value in sorted(env_vars.items()):
        print(f"{key}: {value}")
    print("-" * 120)
    print(f"Total environment variables: {len(env_vars)}")

def check_required_variables() -> bool:
    """
    Checks whether all required environment variables are set and point to valid directories.

    Returns:
        bool: True if all required environment variables are set and their paths exist, False otherwise.
    """
    for var in REQ_VARIABLES:
        value = os.environ.get(var)
        if not value or not os.path.exists(value):
            return False
    return True

def check_variable_set(var_name: str) -> bool:
    """
    Checks whether a specific environment variable is set and points to a valid directory.

    Args:
        var_name (str): The name of the environment variable to check.
    Returns:
        bool: True if the environment variable is set and the path exists, False otherwise. 
    """
    value = os.environ.get(var_name)
    if value and os.path.exists(value):
        return True
    return False
