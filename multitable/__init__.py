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

from functools import reduce

# CONSTANT PARAMETERS FOR MODULE

module_version = "1.1.0"
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


def concatlist(frames:list[MultiTable], engine:str) -> MultiTable:
    """
    Concat a list of multitable frames

    Args:
        frames (list[MultiTable]): The input list of MultiTable instances to concatenate.
        engine (str): The engine type ("pandas", "polars", "pyspark").

    Raises:
        ValueError: If the list of frames is empty or if an unsupported engine is specified.
        NotImplementedError: If the specified engine is not implemented.

    Returns:
        MultiTable: A new MultiTable instance containing the concatenated data.
    """
    if not frames:
        raise ValueError("No frames to concatenate")
    native_frames = [f.df for f in frames]

    if engine == "pandas":
        combined = pd.concat(native_frames, ignore_index=True)

    elif engine == "polars":
        combined = pl.concat(native_frames)

    elif engine == "pyspark":
        # Safe union across all frames
        if len(native_frames) == 1:
            combined = native_frames[0]
        else:
            combined = reduce(lambda df1, df2: df1.union(df2), native_frames)

    else:
        raise NotImplementedError(
            f"RS400 Metaframe appendage not implemented for backend '{engine}'"
        )
    
    return MultiTable(combined, src_path=frames[0].src_path, table_name=frames[0].table_name, frame_type=engine)
