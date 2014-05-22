yarn.xml cleaner
----------------

Tested on python 2.7
Require pip.

INSTALL
=======

    # apt-get install python-pip
    # pip install -r requirements.txt

USE
===

## How to use as local script:

    $ python cleaner.py --local  # require yarn.xml file, and write result to yarn.cleaned.xml

## How to use with spark:

    $ bin/pyspark cleaner.py local file:///C:\Users\...\yarn.xml

or

    $ bin/pyspark cleaner.py local hdfs:.../yarn.xml

