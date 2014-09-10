import os
import argparse
from mpadi import import_data

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

                print '\n\nImporting from %s\n' % csv_filepath
                doc_count = import_data(csv_filepath, year)

                result_dict[filename] = doc_count

    print "\nResults:"
    for key in result_dict.keys():
        print "%s Kosovo Assembly Deputies from '%s'" % (result_dict[key], key)
