yarn.xml cleaner
----------------

Tested on python 2.7.

Require python 2.x.
Require pip. (for python 2.x)

Require xml line format:
 - Line can contain only one `<example ..> </example>` or `<definition ..> </definition>` tag.
 - Use only single-line tag for `example` and `definition` tags.

Cleaning:
 - `<example ..> </example>` example inner content.
 - `<definition ..> </definition>` definition inner content.
 - `<example source=" " ..> </example>` example source attribute content.

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

