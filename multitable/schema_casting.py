from polars import Date, Int8, Int16, Int32, Int64, Float32, Float64, Utf8, Boolean
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, DateType, BooleanType

#list of all acceptable types for casting
ACCEPTABLE_TYPES = ['int8', 'int16', 'int32', 'int64', 'float32', 'float64', 'string', 'date', 'bool']

# Mapping from string type names to actual types for different frameworks
PYSPARK_TYPE_MAPPING = {
    'int8': ByteType(),
    'int16': ShortType(),
    'int32': IntegerType(),
    'int64': LongType(),
    'float32': FloatType(),
    'float64': DoubleType(),
    'string': StringType(),
    'date': DateType(),
    'bool': BooleanType()
}

POLARS_TYPE_MAPPING = {
    'int8': Int8,
    'int16': Int16,
    'int32': Int32,
    'int64': Int64,
    'float32': Float32,
    'float64': Float64,
    'string': Utf8,
    'date': Date,
    'bool': Boolean
}

def get_data_type_for_backend(backend: str, target_type: str):
    """
    Map the target_type string to the appropriate data type for the specified backend.
    
    Args:
        backend (str): The backend type ('pandas', 'polars', 'pyspark').
        target_type (str): The target data type as a string.
        
    Returns:
        The corresponding data type for the backend.
    """
    if target_type not in ACCEPTABLE_TYPES:
        raise ValueError(f"SC630 target_type must be one of {ACCEPTABLE_TYPES}")
    
    if backend == "pandas":
        return target_type
    elif backend == "polars":
        return POLARS_TYPE_MAPPING[target_type]
    elif backend == "pyspark":
        return PYSPARK_TYPE_MAPPING[target_type]
    else:
        raise NotImplementedError(f"SC631 CastColumnType not implemented for backend '{backend}'")
