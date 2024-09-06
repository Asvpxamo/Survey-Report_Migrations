import polars as pl
import os as os
from datetime import datetime
import re

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

def generateCSV(df, path):
    base_name, extension = os.path.splitext(path)
    current_datetime = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    path = f"{base_name}_{current_datetime}{extension}"
    
    df.write_csv(path, separator=';')
    print(f"CSV generated at {path}")

def processMapping(df):
    #pdf_directory = r"C:\Users\Amani.devos\OneDrive - Circet Benelux NV\Bureaublad\Dataloader\Survey Migration\Final Migration Grimbergen_GavereKruisemOudenaarde\SurveyReports"
    pdf_directory = input("Enter the path to the PDF directory: ").replace('"', '')
    df = df.drop(['Id', 'OwnerName', 'Type', 'OwnerId', 'LastModifiedDate', 'CreatedDate', 'ParentId', 'ContentDocumentLinkId'])
    df = df.sort('ParentName')
        
    df = df.with_columns(
        (pl.lit(pdf_directory) + '/' + df['FilePath'].apply(replace_non_ascii_with_regex) + '.pdf').alias('FilePath'))

    print(df)
    df_new = pl.DataFrame({
        'FirstPublishLocation:sitetracker__Site__c-Name': df['ParentName'],
        'Title': df['Name'].apply(replace_non_ascii_with_regex),
        'PathOnClient': df['FilePath'],
        'VersionData': df['FilePath']
    })
    print(df_new)
    return df_new


replacement_dict = {
    "é": "e", "è": "e", "ô": "o", "ö": "o", "à": "a", "î": "i", "ë": "e", "ç": "c",
    "ê": "e", "ü": "u", "â": "a", "ù": "u", "û": "u", "ï": "i", "ä": "a", "É": "E",
    "È": "E", "Ô": "O", "Ö": "O", "À": "A", "Î": "I", "Ë": "E", "Ç": "C", "Ê": "E",
    "Ü": "U", "Â": "A", "Ù": "U", "Û": "U", "Ï": "I", "Ä": "A", "Ã€": "A", "Ã‚": "A",
    "Ãƒ": "A", 'Ã„': 'A', "Ã…": "A", "Ã†": "AE", "Ã‡": "C", "Ãˆ": "E", "Ã‰": "E",
    "ÃŠ": "E", "Ã‹": "E", "ÃŒ": "I", "ÃŽ": "I", "Ã‘": "N", "Ã’": "O", 'Ã“': "O",
    'Ã”': 'O', "Ã•": "O", "Ã–": "O", "Ã—": "x", "Ã˜": "O", "Ã™": "U", "Ãš": "U",
    "Ã›": "U", "Ãœ": "U", "Ãž": "Th", "ÃŸ": "ss", "Ã¡": "a", "Ã¢": "a", "Ã£": "a",
    "Ã¤": "a", "Ã¥": "a", "Ã¦": "ae", "Ã§": "c", "Ã¨": "e", "Ã©": "e", "Ãª": "e",
    "Ã«": "e", "Ã¬": "i", "Ã­": "i", "Ã®": "i", "Ã¯": "i", "Ã°": "d", "Ã±": "n",
    "Ã²": "o", "Ã³": "o", "Ãµ": "o", "Ã¶": "o", "Ã·": "÷", "Ã¸": "o", "Ã¹": "u",
    "Ãº": "u", "Ã»": "u", "Ã¼": "u", "Ã½": "y", "Ã¾": "th", "Ã¿": "y"
}

# Create a regular expression from the dictionary keys
regex = re.compile("(%s)" % "|".join(map(re.escape, replacement_dict.keys())))

def replace_non_ascii_with_regex(cell):
    return regex.sub(lambda mo: replacement_dict[mo.string[mo.start():mo.end()]], cell)

# generally slower than the regex approach, but less complex
#def replaceNonAscii(cell):
#
#    cell = cell.replace("é", "e").replace("è", "e").replace("ô", "o").replace("ö", "o").replace("à", "a").replace("î", "i").replace("ë", "e").replace("ç", "c").replace("ê", "e").replace("ü", "u").replace("â", "a").replace("ù", "u").replace("û", "u").replace("ï", "i").replace("ä", "a").replace("É", "E").replace("È", "E").replace("Ô", "O").replace("Ö", "O").replace("À", "A").replace("Î", "I").replace("Ë", "E").replace("Ç", "C").replace("Ê", "E").replace("Ü", "U").replace("Â", "A").replace("Ù", "U").replace("Û", "U").replace("Ï", "I").replace("Ä", "A").replace("Ã€", "A").replace("Ã‚", "A").replace("Ãƒ", "A").replace('Ã„', 'A').replace("Ã…", "A").replace("Ã†", "AE").replace("Ã‡", "C").replace("Ãˆ", "E").replace("Ã‰", "E").replace("ÃŠ", "E").replace("Ã‹", "E").replace("ÃŒ", "I").replace("ÃŽ", "I").replace("Ã‘", "N").replace("Ã’", "O").replace('Ã“', "O").replace('Ã”', 'O').replace("Ã•", "O").replace("Ã–", "O").replace("Ã—", "x").replace("Ã˜", "O").replace("Ã™", "U").replace("Ãš", "U").replace("Ã›", "U").replace("Ãœ", "U").replace("Ãž", "Th").replace("ÃŸ", "ss").replace("Ã¡", "a").replace("Ã¢", "a").replace("Ã£", "a").replace("Ã¤", "a").replace("Ã¥", "a").replace("Ã¦", "ae").replace("Ã§", "c").replace("Ã¨", "e").replace("Ã©", "e").replace("Ãª", "e").replace("Ã«", "e").replace("Ã¬", "i").replace("Ã­", "i").replace("Ã®", "i").replace("Ã¯", "i").replace("Ã°", "d").replace("Ã±", "n").replace("Ã²", "o").replace("Ã³", "o").replace("Ãµ", "o").replace("Ã¶", "o").replace("Ã·", "÷").replace("Ã¸", "o").replace("Ã¹", "u").replace("Ãº", "u").replace("Ã»", "u").replace("Ã¼", "u").replace("Ã½", "y").replace("Ã¾", "th").replace("Ã¿", "y").replace('Ã', 'a')
#    
#    return cell

mapping_file_path = input("Enter the path to the mapping file: ").replace('"', '')
df_mapping = readFile(mapping_file_path)
df_mapping = processMapping(df_mapping)

generateCSV(df_mapping, os.path.dirname(mapping_file_path + "/MAPPING-TO-UPLOAD.csv")) 
