import os

# list the filepaths
test_tables_pth = "../test_tables"

def get_test_paths():
    test_tables = [
        os.path.join(test_tables_pth, f)
        for f in os.listdir(test_tables_pth)
        if os.path.isfile(os.path.join(test_tables_pth, f))
    ]
    return test_tables
