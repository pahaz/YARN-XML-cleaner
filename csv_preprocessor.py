import csv
import glob
import re
import sys

__author__ = 'pahaz'


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Use: {0} file.csv".format(sys.argv[0]))

    file_pattern = sys.argv[1]
    if not file_pattern.endswith('.csv'):
        sys.exit('file name don`t contain .csv extension.')

    for file_ in glob.glob(file_pattern):
        out_file_ = file_.replace('.csv', '.pre_cleaned.csv')
        with open(file_, 'rb') as csvfile:
            with open(out_file_, 'wb') as out_csvfile:
                csvfile.readline()  # mess headers 
                writer = csv.writer(out_csvfile, strict=True, quoting=csv.QUOTE_MINIMAL)
                reader = csv.reader(csvfile, strict=True)

                try:
                    for row in reader:
                        new_row = [re.sub(u'\\n', '', x) for x in row]
                        writer.writerow(new_row)
                except csv.Error as e:
                    sys.exit('file %s, line %d: %s' % (file_, reader.line_num, e))

