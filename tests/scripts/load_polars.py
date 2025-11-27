#append ../../transformslib package
import os
import sys
sys.path.append("../..")
from multitable import MultiTable, FrameTypeVerifier

# list the filepaths
test_tables_pth = "../test_tables"
test_tables = [
    os.path.join(test_tables_pth, f)
    for f in os.listdir(test_tables_pth)
    if os.path.isfile(os.path.join(test_tables_pth, f))
]

for t in test_tables:
    print(f"Attempting load of... {t}")
    # get extension safely
    _, file_extension = os.path.splitext(t)
    file_extension = file_extension.lstrip(".")  # remove leading dot

    mt = MultiTable.load(t, format=file_extension, frame_type=FrameTypeVerifier.polars)

    mt.show()
    print(f"Number of columns: {mt.nvars}")
    print(f"Number of entries: {mt.nrow}")
