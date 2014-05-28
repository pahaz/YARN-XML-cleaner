import glob
import gzip
import sys

__author__ = 'pahaz'


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("Use: {0} file.gz")

    file_pattern = sys.argv[1]
    if not file_pattern.endswith('.gz'):
        sys.exit('file name don`t contain .gz extension.')

    for file_ in glob.glob(file_pattern):
        out_file_ = file_.replace('.gz', '')
        with gzip.open(file_, 'rb') as gzfile:
            with open(out_file_, 'wb') as out:
                file_content = gzfile.read()
                out.write(file_content)
