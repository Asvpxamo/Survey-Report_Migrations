import polars as pl
import os as os
import glob
from datetime import datetime

def readFile(path):
    if path.endswith('.csv'):
        df = pl.read_csv(path, truncate_ragged_lines=True) #truncate_ragged_lines=True is used to avoid errors when reading csv files with different number of columns, it will fill the missing columns with null values
        print(f"Read {path} succesfully")
    elif path.endswith('.xlsx'):
        df = pl.read_excel(path)
        print(f"Read {path} succesfully")
    else:
        raise ValueError('File type not supported')
    return df

def concatFiles(dir):
    files = glob.glob(os.path.join(os.getcwd() + dir, '*ContentDocumentId*.csv'))
    dfs = []
    for i, file in enumerate(files):
        dfs.append(readFile(file))
        print(f"shape: {dfs[i].shape}")
    return pl.concat(dfs)

def generateCSV(df, path):
    base_name, extension = os.path.splitext(path)
    current_datetime = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    path = f"{base_name}_{current_datetime}{extension}"
    
    df.write_csv(path, separator=';')
    print(f"CSV generated at {path}")

documentIds_df = concatFiles("/contentDocIds")
print(documentIds_df)
generateCSV(documentIds_df, os.getcwd() + '/ALL_documentIds.csv')    #getcwd() returns the current working directory

#globbing is the process of finding files by matching file names with a pattern. The glob module in Python provides a function called glob() that takes a pattern and returns a list of the files that match that pattern. The pattern can contain wildcards like * and ?. For example, the pattern *.txt will match all files with the .txt extension in the current directory. The pattern ??.txt will match all files with two characters followed by the .txt extension. The glob() function can be used to find files that match a specific pattern, which can be useful for processing multiple files in a directory.
