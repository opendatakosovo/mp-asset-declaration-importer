import os
import argparse
from mpadi import import_data

#from mpadi import build_medians_collection

parser = argparse.ArgumentParser()
parser.add_argument(
    '--csvDirectoryPath',
    type=str, default='data',
    help='The CSV directory path.')

args = parser.parse_args()
csv_directory_path = args.csvDirectoryPath


# Run the app
if __name__ == '__main__':
    result_dict = {}

    if os.path.isdir(csv_directory_path):
        for filename in os.listdir(csv_directory_path):
            if(filename.endswith(".csv")):

                csv_filepath = csv_directory_path + '/' + filename
                year = int(filename.replace('.csv', ''))

                print "\n\nImporting %s asset declarations from '%s':\n" % (year, csv_filepath)
                doc_count = import_data(csv_filepath, year)

                result_dict[year] = doc_count

    print "\n\nIMPORT SUMMARY:"
    for key in result_dict.keys():
        print "%s asset declarations imported from %s Kosovo Assembly MPs." % (result_dict[key], key)

    print  # Just skip line
