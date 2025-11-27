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
