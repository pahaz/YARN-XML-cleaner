import sys

__author__ = 'pahaz'

import requests as r


def file_name_from_url(url):
    url = url.replace('\\', '/').strip('/')
    return url.rsplit('/', 1)[-1]


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == '-f':
        with open(sys.argv[2]) as f:
            urls = [x.strip() for x in f.readlines() if x.strip()]

    elif len(sys.argv) >= 2 and '-f' not in sys.argv:
        urls = sys.argv[1:]

    else:
        sys.exit("Use: {0} (-f file_with_urls.txt | url1 url2 ..)")

    for url in urls:
        print('Url: {0}'.format(url))
        rez = r.get(url)
        len_ = int(rez.headers['content-length'])
        count_parts = int(len_ / 1024*10)
        file_name = file_name_from_url(url)
        print("Download [0/{2}]: {1}".format(
            url, file_name, count_parts))
        with open(file_name, 'wb') as fd:
            for chunk in rez.iter_content(chunk_size=1024*10):
                print '*',
                fd.write(chunk)

        print(' ok.')
