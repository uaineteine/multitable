#append ../../transformslib package
import os
import sys
sys.path.append("../..")
from multitable import MultiTable, FrameTypeVerifier
from get_test_data import get_test_paths
from tabulate import tabulate

test_tables = get_test_paths()
print("running tests with polars")
for t in test_tables:
    print(f"Attempting load of... {t}")
    # get extension safely
    _, file_extension = os.path.splitext(t)
    file_extension = file_extension.lstrip(".")  # remove leading dot

    mt = MultiTable.load(t, format=file_extension, frame_type=FrameTypeVerifier.polars)

    mt.show()
    print(f"Number of columns: {mt.nvars}")
    print(f"Number of entries: {mt.nrow}")
    print("Data types:")
    print(mt.dtypes)

    print("")
    print("Test copying")
    mt_copy = mt.copy()
    mt_copy.show()

    #apply test of trimming
    print("Testing trimming of string columns...")
    for col in mt.columns:
        mt.trimwhite()
    mt.show()

    #test some basic functionality
    print("Testing conversions on frame...")

    print("De-dup / distinct checks")
    mt = mt.distinct()

    print("Testing the sort")
    mt = mt.sort(by=mt.columns)

    print("Testing pandas conversion")
    pd_df = mt.get_pandas_frame()
    print(tabulate(pd_df, headers="keys", tablefmt="pretty", showindex=False))

    mt.write(f"../test_tables/test_outputs/polars/{os.path.basename(t)}.parquet", "parquet")

sys.exit(0)
