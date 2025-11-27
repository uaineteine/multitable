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
