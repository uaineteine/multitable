import os
import re
from math import ceil
from typing import Union, List
import polars as pl
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql.types import ByteType, BooleanType, ShortType, IntegerType, FloatType, LongType, DoubleType, TimestampType, DecimalType, StringType, BinaryType, DateType, ArrayType, MapType, StructType
from pyspark.sql.functions import concat_ws, col, explode, explode_outer, split, round as spark_round, trim, regexp_replace

#module imports
from naming_standards import Tablename
from .frame_check import FrameTypeVerifier
from .load_dfs import _load_spark_df, _load_pandas_df, _load_polars_df
from adaptiveio.pathing import normalisePaths
from itables import show as interactive_show

def spark_size_per_type(dtype) -> int:
    """
    Function to return an estimate size for a data type in use by the user
    """
    
    spark_sizes = {
        ByteType: 8,
        BooleanType: 1,
        ShortType: 16,
        IntegerType: 32,
        FloatType: 32,
        DateType: 32,
        LongType: 64,
        DoubleType: 64,
        TimestampType: 64,
        DecimalType: 16,
        StringType: 32*8,
        BinaryType: 64,
    }
    
    for spark_type, size in spark_sizes.items():
        if isinstance(dtype, spark_type):
            return size
    #implied else
    
    #iterative and composites types
    if isinstance(dtype, ArrayType):
        return spark_size_per_type(dtype.elementType) * 80
    
    if isinstance(dtype, MapType):
        return spark_size_per_type(dtype.elementType) * 80
    
    if isinstance(dtype, StructType):
        return sum(spark_size_per_type(f.dataTye) for f in dtype.fields)
         
    #unkown raise warning - else
    print(f"MT925 cannot estimate datatype of {dtype}. Defaulting to 256 bits for item")
    return 256

class MultiTable: 
    """
    A unified wrapper class for handling DataFrames across different frameworks.
    
    This class provides a consistent interface for working with DataFrames from PySpark, Pandas, 
    and Polars. It includes utility methods for accessing DataFrame properties and converting between formats.
    
    Attributes:
        df: The underlying DataFrame (PySpark DataFrame, Pandas DataFrame, or Polars LazyFrame).
        frame_type (str): The type of DataFrame ('pyspark', 'pandas', 'polars').
        table_name (Tablename): The name of the table, validated and formatted.
        src_path (str): The source file path where the data was loaded from.
        
    Example:
        >>> # Create from existing DataFrame
        >>> import pandas as pd
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        >>> mf = MetaFrame(df, "data.csv", "my_table", "pandas")
        >>> 
        >>> # Load from file
        >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
        >>> 
        >>> # Access properties
        >>> print(f"Columns: {mf.columns}")
        >>> print(f"Rows: {mf.nrow}")
        >>> print(f"Variables: {mf.nvars}")
    """

    def sort_columns(self):
        """
        sort columns alphabetically in-place. Defaults to True.
        """
        # Inline sort for each frame type
        if self.frame_type == "pandas":
            self.df = self.df[sorted(self.df.columns)]
        elif self.frame_type == "polars":
            self.df = self.df.select(sorted(self.df.columns))
        elif self.frame_type == "pyspark":
            self.df = self.df.select(*sorted(self.df.columns))
        return self
    
    @staticmethod
    def infer_table_name(src_path: str) -> str:
        """
        Infer a table name from a file path by extracting the filename without extension.
        
        This method takes a file path and extracts the base filename, removing any file
        extensions to create a valid table name.

        Args:
            src_path (str): The source file path to extract the table name from.

        Returns:
            str: A Tablename object representing the inferred table name.

        Raises:
            ValueError: If the source path is empty.

        Example:
            >>> name = MetaFrame.infer_table_name("/path/to/my_data.csv")
            >>> print(name)  # "my_data"
            >>> 
            >>> name = MetaFrame.infer_table_name("/path/to/data.parquet")
            >>> print(name)  # "data"
        """
        if src_path == "":
            raise ValueError("MT100 Source path cannot be empty")
        
        #remove trailing / at the end if it exists, but only the last character
        src_path = normalisePaths(src_path)

        #does it have a file extension?
        bn = os.path.basename(src_path)

        if bn == "":
            raise ValueError(f"MT102 source path '{src_path}' is extracting a blank basename")

        #print(bn)
        if bn.find(".") == -1:
            return Tablename(bn)
        else:
            return Tablename(os.path.splitext(bn)[0])

    def __init__(self, df: Union[pd.DataFrame, pl.DataFrame, SparkDataFrame], src_path: str = "", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark):
        """
        Initialise a MultiTable with a DataFrame and metadata.

        Args:
            df: The DataFrame to wrap (PySpark DataFrame, Pandas DataFrame, or Polars LazyFrame).
            src_path (str, optional): The source file path. Defaults to "".
            table_name (str, optional): The name for the table. If empty, will be inferred from src_path.
                                      Defaults to "".
            frame_type (str, optional): The type of DataFrame framework. Defaults to "pyspark".
                                      Supported types: "pyspark", "pandas", "polars".

        Raises:
            ValueError: If the DataFrame type doesn't match the specified frame_type.
            ValueError: If table_name is invalid when provided.

        Example:
            >>> import pandas as pd
            >>> df = pd.DataFrame({'A': [1, 2, 3]})
            >>> mf = MultiTable(df, "data.csv", "my_table", "pandas")
            >>> 
            >>> # Let table name be inferred
            >>> mf = MultiTable(df, "data.csv", frame_type="pandas")
        """
        #verify the frame type
        FrameTypeVerifier.verify(df, frame_type)

        #store the dataframe and type
        self.df = df
        self.frame_type = frame_type

        if table_name != "":
            self.table_name = Tablename(table_name)
        else:
            self.table_name = MultiTable.infer_table_name(src_path)

        self.src_path = src_path

    def __repr__(self):
        """
        Return a string representation of the MultiTable.
        
        Returns:
            str: The table name as a string representation.
        """
        return self.table_name

    def __str__(self):
        """
        Return a detailed string representation of the MultiTable.
        
        Returns:
            str: A JSON-like string with MultiTable metadata.
        """
        #JSON like string representation
        return f"MultiTable(name={self.table_name}, type={self.frame_type})"

    def rename_table(self, new_name: str):
        """
        Rename the MultiTable's table name in place and log the event.

        Args:
            new_name (str): The new name to assign to the table.

        Returns:
            None

        Raises:
            ValueError: If the new_name is empty or invalid.
        """
        if not new_name or not isinstance(new_name, str):
            raise ValueError("Table name must be a non-empty string")

        old_name = str(self.table_name)
        self.table_name = Tablename(new_name)
        # No return; operation is inplace

    @property
    def columns(self):
        """
        Get the column names of the DataFrame.
        
        This property provides a unified way to access column names across different
        DataFrame frameworks.

        Returns:
            list: List of column names.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(mf.columns)  # ['col1', 'col2', 'col3']
        """
        if self.frame_type == "pyspark":
            return self.df.columns
        elif self.frame_type == "pandas":
            return list(self.df.columns)
        elif self.frame_type == "polars":
            return self.df.columns
        else:
            raise ValueError("Unsupported frame_type")

    @property
    def nvars(self):
        """
        Get the number of variables (columns) in the DataFrame.
        
        This property provides a unified way to get the column count across different
        DataFrame frameworks.

        Returns:
            int: Number of columns in the DataFrame.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(f"Number of variables: {mf.nvars}")
        """
        if self.frame_type == "pyspark":
            return len(self.df.columns)
        elif self.frame_type == "pandas":
            return len(self.df.columns)
        elif self.frame_type == "polars":
            return len(self.df.columns)
        else:
            raise ValueError("Unsupported frame_type")

    @property
    def nrow(self):
        """
        Get the number of rows in the DataFrame.
        
        This property provides a unified way to get the row count across different
        DataFrame frameworks. Note that for PySpark, this triggers an action.

        Returns:
            int: Number of rows in the DataFrame.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MultiTable.load("data.parquet", "parquet", "my_table", "pandas")
            >>> print(f"Number of rows: {mf.nrow}")
        """
        if self.frame_type == "pyspark":
            return self.df.count()
        elif self.frame_type == "pandas":
            return len(self.df)
        elif self.frame_type == "polars":
            if isinstance(self.df, pl.LazyFrame):
                return self.df.select(pl.count()).collect().item()
            else:
                return len(self.df)
        else:
            raise ValueError("Unsupported frame_type")

    def remove_character(self, col_to_alter:str, val_to_remove:str, index:str):
        """
        Method to remove characters from a column in a MultiTable. This is used by the transforms method to apply the transformation to the appropriate backend.

        Args:
            col_to_alter (str): The name of the column to alter
            val_to_remove (str): The character(s) to remove from the column values
            index (str): "all" to remove all occurrences, "left" to remove only if it's the first character, "right" to remove only if it's the last character.
        """
        col_type = self.dtypes[col_to_alter]
        backend = self.frame_type

        if backend == "pyspark":
            # If var_to_alter is not of StringType, the original DataFrame gets returned
            if self.dtypes[col_to_alter] != 'StringType()':
                raise KeyError(f"MT630 Column '{col_to_alter}' is of type '{col_type}' which is not a string type, cannot remove characters. Please check your column name and types.")

            if index == "all":
                # Remove all occurrences of val_to_remove
                self.df = self.df.withColumn(col_to_alter, regexp_replace(col(col_to_alter), val_to_remove, ""))
                print(f"Removed all occurances of {val_to_remove} from {col_to_alter}")

            elif index == "left":
                # Remove val_to_remove if its the first occurrence in the string
                self.df = self.df.withColumn(col_to_alter, regexp_replace(col(col_to_alter), f"^{val_to_remove}+", ""))
                print(f"Removed the first occurance of {val_to_remove} from {col_to_alter}")

            elif index == "right":
                # Remove val_to_remove if its the last occurrence in the string
                self.df = self.df.withColumn(col_to_alter, regexp_replace(col(col_to_alter), f"({val_to_remove})+$", ""))
                print(f"Removed the last occurance of {val_to_remove} from {col_to_alter}")

            else:
                raise ValueError("MT632 index must be 'all', 'left' or 'right'")
        else:
            raise NotImplementedError(f"MT631 RemoveCharacters not implemented for backend '{backend}'")

    def get_pandas_frame(self) -> pd.DataFrame:
        """
        Convert the DataFrame to a Pandas DataFrame.
        
        This method provides a unified way to convert any supported DataFrame type
        to a Pandas DataFrame for analysis or processing.

        Returns:
            pd.DataFrame: A Pandas DataFrame representation of the data.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pyspark", spark)
            >>> pandas_df = mf.get_pandas_frame()
            >>> print(type(pandas_df))  # <class 'pandas.core.frame.DataFrame'>
        """
        if self.frame_type == "pyspark":
            return self.df.toPandas()
        elif self.frame_type == "pandas":
            print("WARNING: Unoptimised code, DataFrame is already a Pandas DataFrame.")
            return self.df
        elif self.frame_type == "polars":
            if isinstance(self.df, pl.LazyFrame):
                return self.df.collect().to_pandas()
            else:
                return self.df.to_pandas()
        else:
            raise ValueError("Unsupported frame_type")

    def get_polars_lazy_frame(self) -> pl.LazyFrame:
        """
        Convert the DataFrame to a Polars LazyFrame.
        
        This method provides a unified way to convert any supported DataFrame type
        to a Polars LazyFrame for lazy evaluation and optimization.

        Returns:
            pl.LazyFrame: A Polars LazyFrame representation of the data.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> lazy_frame = mf.get_polars_lazy_frame()
            >>> print(type(lazy_frame))  # <class 'polars.lazyframe.frame.LazyFrame'>
        """
        if self.frame_type == "pyspark":
            pd_df = self.get_pandas_frame()
            return pl.from_pandas(pd_df)
        elif self.frame_type == "polars":
            print("WARNING: Unoptimised code, DataFrame is already a polars LazyFrame.")
            return self.df
        elif self.frame_type == "pandas":
            return pl.from_pandas(self.df).lazy()
        else:
            raise ValueError("Unsupported frame_type")
    
    def get_spark_frame(self, spark:SparkSession) -> SparkDataFrame:
        """
        Convert the DataFrame to a pyspark DataFrame.
        
        This method provides a unified way to convert any supported DataFrame type
        to a spark DataFrame for lazy evaluation and optimisation.

        Returns:
            pyspark.sql.DataFrame: A pyspark DataFrame representation of the data.

        Raises:
            ValueError: If the frame_type is not supported.
        """
        if self.frame_type == "pyspark":
            print("WARNING: Unoptimised code, DataFrame is already a pyspark DataFrame.")
            return self.df
        elif self.frame_type == "polars":
            print("WARNING: Performance of polars -> pandas -> pyspark is poorly optimised")
            raise ValueError("Unsupported conversion")
        elif self.frame_type == "pandas":
            return spark.createDataFrame(self.df)
        else:
            raise ValueError("Unsupported frame_type")

    def select(self, *columns):
        """
        Select specific columns from the DataFrame.

        Accepts multiple column names as separate args, or a single list/tuple:
            select("col1", "col2")
            select(["col1", "col2"])
        """
        # allow either select("a","b") or select(["a","b"])
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            cols = list(columns[0])
        else:
            cols = list(columns)

        if not cols:
            raise ValueError("No columns provided to select()")

        # Validate columns exist
        self.validate_columns_exist(cols)

        if self.frame_type == "pandas":
            self.df = self.df[cols]
        elif self.frame_type == "polars":
            self.df = self.df.select(cols)
        elif self.frame_type == "pyspark":
            self.df = self.df.select(*cols)
        else:
            raise ValueError("MT004 Unsupported frame_type to use select method")

        return self
        
    def validate_columns_exist(self, columns):
        """
        Validate that the provided columns exist in the DataFrame.

        Args:
            columns (list): List of column names to validate.

        Raises:
            ValueError: If any column does not exist in the DataFrame.
        """
        if self.frame_type == "pandas":
            missing = [col for col in columns if col not in self.df.columns]
        elif self.frame_type == "polars":
            missing = [col for col in columns if col not in self.df.schema]
        elif self.frame_type == "pyspark":
            missing = [col for col in columns if col not in self.df.columns]
        else:
            raise ValueError("MT005 Unsupported frame_type to validate columns")

        if missing:
            raise ValueError(f"The following columns are missing: {', '.join(missing)}")

    def show(self, n: int = 20, truncate: bool = True):
        """
        Display the DataFrame content in a formatted way.
        
        This method provides a unified way to display DataFrame content across different
        frameworks. It handles the different display behaviors of PySpark, Pandas, and Polars.

        Args:
            n (int, optional): Number of rows to display. Defaults to 20.
            truncate (bool, optional): Whether to truncate long strings. Defaults to True.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf = MetaFrame.load("data.parquet", "parquet", "my_table", "pandas")
            >>> mf.show()  # Display first 20 rows
            >>> mf.show(10)  # Display first 10 rows
            >>> mf.show(50, truncate=False)  # Display first 50 rows without truncation
        """
        if self.frame_type == "pyspark":
            self.df.show(n=n, truncate=truncate)
        elif self.frame_type == "pandas":
            print(f"MultiTable: {self.table_name} ({self.frame_type})")
            print(f"Shape: {self.df.shape[0]} rows × {self.df.shape[1]} columns")
            print(self.df.head(n))
        elif self.frame_type == "polars":
            print(f"MultiTable: {self.table_name} ({self.frame_type})")
            print(self.df.head(n))
        else:
            raise ValueError("Unsupported frame_type")
    
    @staticmethod
    def load_native_df(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, spark=None):
        """
        Load a DataFrame from a file and return a DataFrame instance.

        This static method provides a convenient way to load data from various file formats and create a dataframe. It supports multiple file formats and DataFrame frameworks.

        Parameters
        ----------
        path : str
            Path to the data file to load.
        format : str, optional
            File format of the data. Defaults to "parquet". Supported formats: "parquet", "csv", "sas".
        table_name : str, optional
            Name to assign to the table. If empty, will be inferred from the file path. Defaults to "".
        frame_type : str, optional
            Type of DataFrame to create. Defaults to "pyspark". Supported types: "pyspark", "pandas", "polars".
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns
        -------
        dataframe : pyspark.sql.DataFrame or pandas.DataFrame or polars.LazyFrame
            A pyspark, polars or pandas dataframe.

        Raises
        ------
        FileNotFoundError
            If the specified path does not exist.
        ValueError
            If the format or frame_type is not supported, or if spark is None when frame_type is "pyspark".
        Exception
            If there are issues loading the data.
        """
        if frame_type == "pyspark":
            df = _load_spark_df(path, format, table_name, spark)
        elif frame_type == "pandas":
            df = _load_pandas_df(path, format, table_name)
        elif frame_type == "polars":
            df = _load_polars_df(path, format, table_name)
        else:
            raise ValueError("Unsupported frame_type")
        return df

    @staticmethod
    def load(path:str, format: str = "parquet", table_name: str = "", frame_type: str = FrameTypeVerifier.pyspark, auto_lowercase = False, spark=None):
        """
        Load a DataFrame from a file and return a MultiTable instance.

        This static method provides a convenient way to load data from various file formats and create a MultiTable with appropriate metadata. It supports multiple file formats and DataFrame frameworks.

        Parameters
        ----------
        path : str
            Path to the data file to load.
        format : str, optional
            File format of the data. Defaults to "parquet". Supported formats: "parquet", "csv", "sas".
        table_name : str, optional
            Name to assign to the table. If empty, will be inferred from the file path. Defaults to "".
        frame_type : str, optional
            Type of DataFrame to create. Defaults to "pyspark". Supported types: "pyspark", "pandas", "polars".
        auto_lowercase : bool, optional
            Automatically lowercase column names. Defaults to False.
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Returns
        -------
        MultiTable
            A new MultiTable instance with the loaded data and metadata.

        Raises
        ------
        FileNotFoundError
            If the specified path does not exist.
        ValueError
            If the format or frame_type is not supported, or if spark is None when frame_type is "pyspark".
        Exception
            If there are issues loading the data.

        Examples
        --------
        >>> # Load a PySpark DataFrame
        >>> mf = MultiTable.load("data.parquet", format="parquet", table_name="my_table", frame_type="pyspark", spark=spark)
        >>> # Load a Pandas DataFrame
        >>> mf = MultiTable.load("data.csv", format="csv", table_name="my_table", frame_type="pandas")
        >>> # Load a Polars DataFrame
        >>> mf = MultiTable.load("data.parquet", format="parquet", table_name="my_table", frame_type="polars")
        """
        df = MultiTable.load_native_df(path=path, format=format, table_name=table_name, frame_type=frame_type, spark=spark)

        if auto_lowercase:
            if frame_type == "pandas":
                df.columns = [col.lower() for col in df.columns]
            elif frame_type == "polars":
                df = df.rename({col: col.lower() for col in df.columns})
            elif frame_type == "pyspark":
                for old_col in df.columns:
                    df = df.withColumnRenamed(old_col, old_col.lower())
            else:
                raise ValueError("Unsupported frame_type for auto_lowercase")

        mf = MultiTable(df, src_path=path, table_name=table_name, frame_type=frame_type)
        
        return mf

    def display(self):
        """
        Display the DataFrame content in an interactive way.
        
        This method provides a unified way to display DataFrame content across different
        frameworks. It handles the different display behaviors of PySpark, Pandas, and Polars.

        Raises:
            ValueError: If the frame_type is not supported.

        Example:
            >>> mf.display()  # Display first 20 rows
        """
        if self.frame_type == "pyspark":
            self.df.display()
        elif self.frame_type == "pandas":
            interactive_show(self.df)
        elif self.frame_type == "polars":
            if self.nrow < 1000:
                df = self.get_pandas_frame()
                interactive_show(df)
            else:
                print("Interactive display unsupported for > 1000 rows via polars")
                raise ValueError("MF981 Unsupported frame_type for display")
        else:
            raise ValueError("MF980 Unsupported frame_type for display")

    def drop(self, columns: Union[str, list]):
        """
        Drop one or more columns from the DataFrame.
        
        Args:
            columns (Union[str, list]): Column name or list of column names to drop.
        
        Raises:
            ValueError: If the frame_type is unsupported.
        
        Example:
            >>> mf2 = mf.drop("col1")  # Drop a single column
            >>> mf3 = mf.drop(["col1", "col2"])  # Drop multiple columns
        """
        if isinstance(columns, str):
            columns = [columns]

        if self.frame_type == "pandas":
            new_df = self.df.drop(columns=columns)
        elif self.frame_type == "polars":
            new_df = self.df.drop(columns)  # LazyFrame drop supports list of columns
        elif self.frame_type == "pyspark":
            new_df = self.df
            for col in columns:
                new_df = new_df.drop(col)
        else:
            raise ValueError("Unsupported frame_type")

        self.df = new_df

    def copy(self, new_name: str = None):
        """
        Create a deep copy of the multitable.
        
        Args:
            new_name (str, optional): New name for the copied MultiTable. 
                                      If None, retains the original name. Defaults to None.

        Returns:
            MultiTable: A new MultiTable instance with copied data and events.
        """
        if new_name is None:
            new_name = self.table_name

        newdf = None
        #make a copy to newdf based on frame type
        if self.frame_type == "pandas":
            newdf = self.df.copy(deep=True)
        elif self.frame_type == "polars":
            newdf = self.df.clone()
        elif self.frame_type == "pyspark":
            newdf = self.df.select("*")
        else:
            raise ValueError("Unsupported frame_type for copy")
        
        return MultiTable(newdf, src_path=self.src_path, table_name=str(new_name), frame_type=self.frame_type)
    
    def distinct(self, subset: Union[str, list, None] = None):
        """
        Return a new MultiTable with distinct rows.
        
        Args:
            subset (Union[str, list, None], optional): 
                Column name or list of column names to consider for distinct.
                If None, considers all columns.
        
        Returns:
            MultiTable: A new MultiTable with distinct rows.
        
        Raises:
            ValueError: If the frame_type is unsupported.
        
        Example:
            >>> mf2 = mf.distinct()  # distinct across all columns
            >>> mf3 = mf.distinct("col1")  # distinct by one column
            >>> mf4 = mf.distinct(["col1", "col2"])  # distinct by multiple columns
        """
        if isinstance(subset, str):
            subset = [subset]

        if self.frame_type == "pandas":
            new_df = self.df.drop_duplicates(subset=subset)
        elif self.frame_type == "polars":
            if subset is None:
                new_df = self.df.unique()
            else:
                new_df = self.df.unique(subset)
        elif self.frame_type == "pyspark":
            if subset is None:
                new_df = self.df.distinct()
            else:
                new_df = self.df.dropDuplicates(subset)
        else:
            raise ValueError("Unsupported frame_type")

        self.df = new_df
        
        return self

    def rename(self, columns: dict, inplace: bool = True):
        """
        Rename columns in the DataFrame.

        Args:
            columns (dict): Mapping from old column names to new column names.
            inplace (bool): If True, modifies the current MultiTable. If False, returns a new MultiTable.

        Returns:
            MultiTable or None: Returns a new MultiTable if inplace=False, else None.
        """
        if self.frame_type == "pandas":
            new_df = self.df.rename(columns=columns)
        elif self.frame_type == "polars":
            new_df = self.df.rename(columns)
        elif self.frame_type == "pyspark":
            new_df = self.df
            for old_col, new_col in columns.items():
                new_df = new_df.withColumnRenamed(old_col, new_col)
        else:
            raise ValueError("Unsupported frame_type")

        if inplace:
            self.df = new_df
            return None
        else:
            return MultiTable(new_df, src_path=self.src_path, table_name=str(self.table_name), frame_type=self.frame_type)

    @staticmethod
    def write_native_df(dataframe, path:str, format: str = "parquet", frame_type: str = FrameTypeVerifier.pyspark, overwrite: bool = True, repart_no:int=4, part_on:list[str]=[], spark=None):
        """
        Write a DataFrame to a file in the specified format.
        """
        try:
            if frame_type == "pyspark":
                if spark is None:
                    raise ValueError("MT205 SparkSession required for PySpark")

                #options as an argument for write

                row_limit_per_file = 0

                #TODO fix flexibility of overwrite step
                mode = "overwrite" if overwrite else "error"

                print(f"Writing to {path} as {format} with mode={mode} (compression=zstd)")
                
                if format == "parquet":
                    if part_on != []:
                        dataframe.repartition(repart_no).write.mode(mode).option("maxRecordsPerFile", row_limit_per_file).option("compression", "zstd").format(format).partitionBy(part_on).save(path)
                    else:
                        dataframe.repartition(repart_no).write.mode(mode).option("maxRecordsPerFile", row_limit_per_file).option("compression", "zstd").format(format).save(path)
                elif format == "delta":
                    if part_on != []:
                        dataframe.repartition(repart_no).write.mode(mode).option("maxRecordsPerFile", row_limit_per_file).option("compression", "zstd").format(format).partitionBy(part_on).save(path)
                    else:
                        dataframe.repartition(repart_no).write.mode(mode).option("maxRecordsPerFile", row_limit_per_file).option("compression", "zstd").format(format).save(path)
                else:
                    if part_on != []:
                        raise ValueError("MT400 parting dataset not supported on this format: {format}")
                    else:
                        dataframe.write.mode(mode).option("maxRecordsPerFile", row_limit_per_file).format(format).save(path)

            elif frame_type == "pandas":
                if part_on != []:
                    raise ValueError("MT401 parting dataset not supported on this format: {format}")
                
                if os.path.exists(path) and not overwrite:
                    raise FileExistsError(f"MT501 File {path} already exists and overwrite is False.")
                if format == "parquet":
                    dataframe.to_parquet(path, index=False, compression="zstd")
                else:
                    dataframe.to_parquet(path, index=False)
            
            elif frame_type == "polars":
                if part_on != []:
                    raise ValueError("MT402 parting dataset not supported on this format: {format}")
                
                if os.path.exists(path) and not overwrite:
                    raise FileExistsError(f"MT502 File {path} already exists and overwrite is False.")
                if format == "parquet":
                    dataframe.sink_parquet(path, compression="zstd", compression_level=1)
                else:
                    dataframe.sink_parquet(path)
        except Exception as e:
            print(f"MT800 cannot write file out. PATH {path} FORMAT {format}. EXCEPTION {e}")

    def write(self, path:str, format: str = "parquet", overwrite: bool = True, repart_no:int=4, part_on:list[str]=[], spark=None):
        """
        Write the DataFrame to a file in the specified format.

        Parameters
        ----------
        path : str
            Destination file path.
        format : str, optional
            File format to write. Defaults to "parquet". Supported formats: "parquet", "csv", "sas" (for PySpark).
        overwrite : bool, optional
            If True, overwrites existing files. Defaults to True.
        repart_no: int, optional
            If set, will create the number of target parts to output
        part_on: list, optional
            If set, the outputs via an engine will be parted (if supported)
        spark : optional
            SparkSession object (required for PySpark frame_type). Defaults to None.

        Raises
        ------
        ValueError
            If the frame_type is unsupported.
        FileExistsError
            If the file exists and overwrite is False.
        """
        #error check the part columns
        for p in part_on:
            if p not in self.columns:
                raise ValueError(f"MT600 there is no column {p} in df. Please part on existing columns only: {self.columns}")

        MultiTable.write_native_df(self.df, path, format, self.frame_type, overwrite, part_on=part_on, spark=spark)

    def trimwhite(self, column:str):
        """
        Trim leading and trailing whitespace from specified string columns in-place.

        Args:
            column (str): Column name to trim.
        """
        if self.frame_type == "pandas":
            self.df[column] = self.df[column].str.strip()

        elif self.frame_type == "polars":
            self.df = self.df.with_columns(
                pl.col(column).str.strip_chars().alias(column)
            )

        elif self.frame_type == "pyspark":
            self.df = self.df.withColumn(
                column, trim(col(column))
            )

    def concat(self, new_col_name: str, columns: list, sep: str = "_"):
        """
        Concatenate multiple columns into a single column with a custom separator,
        modifying the current MultiTable in-place.

        Args:
            new_col_name (str): Name of the resulting concatenated column.
            columns (list): List of column names to concatenate.
            sep (str, optional): Separator to use between values. Defaults to "_".

        Raises:
            ValueError: If the frame_type is unsupported.
        """
        if self.frame_type == "pandas":
            self.df[new_col_name] = self.df[columns].astype(str).agg(sep.join, axis=1)

        elif self.frame_type == "polars":
            exprs = [pl.col(column_name).cast(pl.Utf8) for column_name in columns]
            new_expr = pl.concat_str(exprs, separator=sep).alias(new_col_name)
            self.df = self.df.with_columns(new_expr)

        elif self.frame_type == "pyspark":
            print(columns)
            print(sep)
            new_expr = concat_ws(sep, *[col(column_name).cast("string") for column_name in columns])
            self.df = self.df.withColumn(new_col_name, new_expr)

        else:
            raise ValueError("Unsupported frame_type for concat")

    def explode(self, column: str, sep: str | None = None, outer: bool = False):
        """
        Explode (flatten) a column containing lists/arrays (or delimited strings) into multiple rows.
        Always modifies the current MultiTable in place.

        Args:
            column (str): Column name to explode.
            sep (str | None): If provided, split strings in the column by this separator before exploding.
            outer (bool): If True, performs an 'outer explode' (keeps rows where the column is null/empty).

        Raises:
            ValueError: If the frame_type is unsupported.
        """
        if self.frame_type == "pandas":
            if sep:
                self.df[column] = self.df[column].str.split(sep)

            if outer:
                # Pandas explode already keeps NaN, so it's effectively outer
                self.df = self.df.explode(column, ignore_index=True)
            else:
                # dropna ensures we mimic non-outer explode
                self.df = self.df.explode(column, ignore_index=True).dropna(subset=[column])

        elif self.frame_type == "polars":
            if sep:
                self.df = self.df.with_columns(pl.col(column).str.split(sep))

            if outer:
                # Polars explode keeps nulls, so same as outer
                self.df = self.df.with_columns(pl.col(column).explode())
            else:
                # filter out nulls to mimic non-outer explode
                self.df = (
                    self.df.filter(pl.col(column).is_not_null())
                    .with_columns(pl.col(column).explode())
                )

        elif self.frame_type == "pyspark":
            if sep:
                # Escape regex special characters for PySpark split function
                escaped_sep = re.escape(sep)
                self.df = self.df.withColumn(column, split(col(column), escaped_sep))

            if outer:
                self.df = self.df.withColumn(column, explode_outer(col(column)))
            else:
                self.df = self.df.withColumn(column, explode(col(column)))

        else:
            raise ValueError("Unsupported frame_type for explode")

        return None

    def sort(self, by: Union[str, List[str]], ascending: Union[bool, List[bool]] = True) -> "MultiTable":
        """
        Sort the DataFrame by one or more columns in-place.
        Works with pandas, polars, and pyspark DataFrames.

        Args:
            by (str | list[str]): Column name or list of column names to sort by.
            ascending (bool | list[bool], optional): Sort order.
                True for ascending, False for descending.
                Can be a single bool or a list matching the columns. Defaults to True.

        Returns:
            MultiTable: The current MultiTable instance with sorted data (for chaining).

        Raises:
            ValueError: If the length of 'ascending' does not match length of 'by',
                        or if the frame_type is unsupported.

        Example:
            >>> mt.sort("col1")
            >>> mt.sort(["col1", "col2"], ascending=[True, False])
            >>> mt.sort("col1").sort("col2", ascending=False)  # chaining
        """
        if isinstance(by, str):
            by = [by]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(by)
        if len(ascending) != len(by):
            raise ValueError("Length of 'ascending' must match length of 'by' columns.")

        if self.frame_type == "pandas":
            self.df = self.df.sort_values(by=by, ascending=ascending)
        elif self.frame_type == "polars":
            # Polars expects descending, so invert ascending
            self.df = self.df.sort(by, descending=[not asc for asc in ascending])
        elif self.frame_type == "pyspark":
            sort_cols = [col(c) if asc else col(c).desc() for c, asc in zip(by, ascending)]
            self.df = self.df.orderBy(*sort_cols)
        else:
            raise ValueError("Unsupported frame_type for sort")

        return self

    def round(self, column:str, decimals: int = 0):
        """
        Round numerical columns to a specified number of decimal places in-place.

        Args:
            decimals (int, optional): Number of decimal places to round to. Defaults to 0.
        """
        #error check
        if column not in self.columns:
                raise ValueError(f"MT750 Column '{column}' does not exist in the DataFrame.")
        
        if self.frame_type == "pandas":
            self.df[column] = self.df[column].round(decimals)
        elif self.frame_type == "polars":
            self.df = self.df.with_columns(
                pl.col(column).round(decimals).alias(column)
            )
        elif self.frame_type == "pyspark":
            self.df = self.df.withColumn(
                column, spark_round(col(column), decimals)
            )
    
    def sample(self, n: int = None, frac: float = None, seed: int = None):
        """
        Sample rows from the DataFrame and replace the existing DataFrame inplace.

        Args:
            n (int, optional): Number of rows to sample. Mutually exclusive with `frac`.
            frac (float, optional): Fraction of rows to sample. Mutually exclusive with `n`.
            seed (int, optional): Random seed for reproducibility.

        Returns:
            None
        """
        if n is not None and frac is not None:
            raise ValueError("Specify either `n` or `frac`, not both.")

        if self.frame_type == FrameTypeVerifier.pandas:
            self.df = self.df.sample(n=n, frac=frac, random_state=seed)
        elif self.frame_type == FrameTypeVerifier.polars:
            if frac is not None:
                self.df = self.df.sample(frac=frac, seed=seed)
            else:
                self.df = self.df.sample(n=n, seed=seed)
        elif self.frame_type == FrameTypeVerifier.pyspark:
            if frac is None:
                if n is None:
                    raise ValueError("Must specify either `n` or `frac` for sampling.")
                frac = n / self.df.count()
            self.df = self.df.sample(withReplacement=False, fraction=frac, seed=seed)
        else:
            raise NotImplementedError(f"Sampling not supported for frame type {self.frame_type}")
    
    def estimate_frame_size(self, output_format:str="bits") -> int:
        """
        Estimate the multitable frame size based on rows

        Args:
            output_format (str). bits or bytes. defaults to 'bits'.
        
        Returns:
            int: the dataframe size in bits
        """
        if output_format not in ["bits", "bytes"]:
            raise ValueError("MT400 invalid argument: output_format. This can only be 'bits' or 'bytes'")
        
        #init
        value = -1
        
        if self.frame_type == "polars":
            if isinstance(self.df, pl.LazyFrame):
                col = self.df.collect()
                value = col.estimated_size()
            else:
                value = self.df.estimated_size()
            value = value * 8
            
        elif self.frame_type == "pandas":
            value = self.df.memory_usage(deep=True).sum() * 8
            
        elif self.frame_type == "pyspark":
            n = self.nrow
            schema = self.df.schema
            total = 0
            for field in schema.fields:
                total += spark_size_per_type(field.dataType)
            value = total * 8

        #return bits or bytes
        if output_format == "bytes":
            return ceil(value / 8)
        else:
            return value
    
    @property
    def dtypes(self) -> dict:
        """
        Get the data types of each column in the DataFrame.

        Returns:
            dict: A dictionary where keys are column names and values are their data types.

        Raises:
            ValueError: If the frame_type is unsupported.

        Example:
            >>> mt.schema
            {'age': pl.Int64, 'name': pl.Utf8}  # polars
            {'age': IntegerType(), 'name': StringType()}  # pyspark
        """
        if self.frame_type == "pandas":
            return self.df.dtypes.apply(lambda dtype: dtype.name).to_dict()
        
        elif self.frame_type == "polars":
            return {col: str(self.df.schema[col]) for col in self.df.columns}
        
        elif self.frame_type == "pyspark":
            return {field.name: str(field.dataType) for field in self.df.schema.fields}
        
        else:
            raise ValueError("MT200 Unsupported frame_type")

    @property
    def schema(self) -> dict:
        """
        Alias for dtypes
        """
        return self.dtypes

    def validate_numeric_column(self, column_name: str) -> bool:
        """
        Validate that the target column is numeric.

        Args:
            column_name: Name of the column to check.

        Returns a boolean indicating whether the column is numeric.
        """

        if self.frame_type == "pandas":
            if not pd.api.types.is_numeric_dtype(self.df[column_name]):
                print("MT721 Column data type:", self.df[column_name].dtype)
                return False

        elif self.frame_type == "polars":
            col_dtype = self.df.schema[column_name]
            numeric_types = {pl.Float32, pl.Float64, pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                           pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
            if col_dtype not in numeric_types and not isinstance(col_dtype, pl.Decimal):
                print("MT721 Column data type:", col_dtype)
                return False

        elif self.frame_type == "pyspark":
            dtype_str = self.dtypes.get(column_name, "")
            numeric_dtypes = {'DoubleType()', 'FloatType()', 'IntegerType()', 'LongType()', 
                            'ShortType()', 'ByteType()', 'DecimalType()'}
            if dtype_str not in numeric_dtypes:
                print("MT721 Column data type:", dtype_str)
                return False
        
        #important to return true if validation passes
        return True
    
    def get_values_to_list(self, column_name: str, remove_nulls: bool=False) -> List:
        """
        Extract values from a column and return them as a sorted Python list.

        Args:
            column_name: Name of the column.
            remove_nulls: Whether to remove null values from the list. Defaults to False.

        Returns:
            list: Sorted list of values from the column.
        """
        values = []

        # Warn on large PySpark collects
        if self.frame_type == "pyspark":
            row_count = self.df.count()
            if row_count > 50000:
                print(f"MT730 Warning: get_values_to_list is collecting {row_count} rows into driver memory. This may be slow or cause OOM for large datasets.")

        if remove_nulls:
            if self.frame_type == "pandas":
                values = self.df[column_name].dropna().tolist()
            elif self.frame_type == "polars":
                sel = self.df.select(pl.col(column_name))
                if isinstance(sel, pl.LazyFrame):
                    sel = sel.collect()
                values = sel.drop_nulls().to_series().to_list()
            elif self.frame_type == "pyspark":
                values = [row[0] for row in self.df.select(column_name).dropna().collect()]
            else:
                raise NotImplementedError(f"MT729 Backend '{self.frame_type}' not supported for get_values_to_list")
        else:
            if self.frame_type == "pandas":
                values = self.df[column_name].tolist()
            elif self.frame_type == "polars":
                sel = self.df.select(pl.col(column_name))
                if isinstance(sel, pl.LazyFrame):
                    sel = sel.collect()
                values = sel.to_series().to_list()
            elif self.frame_type == "pyspark":
                values = [row[0] for row in self.df.select(column_name).collect()]
            else:
                raise NotImplementedError(f"MT729 Backend '{self.frame_type}' not supported for get_values_to_list")

        values.sort()
        return values

    def validate_string_column(self, column_name: str) -> bool:
        """
        Validate that the target column is a string type.

        Args:
            column_name: Name of the column to check.

        Returns:
            bool: True if column is string type, False otherwise.
        """
        if self.frame_type == "pandas":
            if not pd.api.types.is_string_dtype(self.df[column_name]):
                print("MT722 Column data type:", self.df[column_name].dtype)
                return False

        elif self.frame_type == "polars":
            col_dtype = self.df.schema[column_name]
            if col_dtype != pl.Utf8:
                print("MT722 Column data type:", col_dtype)
                return False

        elif self.frame_type == "pyspark":
            dtype_str = self.dtypes.get(column_name, "")
            if dtype_str != 'StringType()':
                print("MT722 Column data type:", dtype_str)
                return False

        return True

    def validate_datetime_column(self, column_name: str) -> bool:
        """
        Validate that the target column is a date or datetime type.

        Args:
            column_name: Name of the column to check.

        Returns:
            bool: True if column is date/datetime type, False otherwise.
        """
        if self.frame_type == "pandas":
            if not pd.api.types.is_datetime64_any_dtype(self.df[column_name]):
                print("MT723 Column data type:", self.df[column_name].dtype)
                return False

        elif self.frame_type == "polars":
            col_dtype = self.df.schema[column_name]
            if col_dtype not in {pl.Date, pl.Datetime}:
                print("MT723 Column data type:", col_dtype)
                return False

        elif self.frame_type == "pyspark":
            dtype_str = self.dtypes.get(column_name, "")
            if dtype_str not in {'TimestampType()', 'DateType()'}:
                print("MT723 Column data type:", dtype_str)
                return False

        return True
