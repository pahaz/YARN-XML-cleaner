#!/bin/env python
# coding=utf-8

"""
Use:
    bin\pyspark cleaner.py local hdfs://yarn.xml

"""
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

import csv
import glob

from itertools import imap, ifilter
import os
import re

from BeautifulSoup import BeautifulSoup
import HTMLParser
import sys

__author__ = 'pahaz'


# use only for filtering
RE_XML_DEF = re.compile(r'<definition([^>]*)>([^<]*)<', re.UNICODE)
RE_XML_EX = re.compile(r'<example([^>]*)>([^<]*)<', re.UNICODE)

# [^а-яА-Яa-zA-Z0-9-ёЁ«»;//?—― ,.\n()!:]
FIXES = [
    (u'кого-\\W', u'кого-л.'),
    (u'кем-\\W', u'кем-л.'),
    (u'чему-\\W', u'чему-л.'),
    (u'чего-\\W', u'чего-л.'),
    (u'ком-\\W', u'ком-л.'),

    (u'кого-л.', u'кого-либо'),
    (u'кем-л.', u'кем-либо'),
    (u'чему-л.', u'чему-либо'),
    (u'чего-л.', u'чего-либо'),
    (u'ком-л.', u'ком-либо'),

    (u'\\[\\d*\\]', u''),  # [123213] -> ''
    (u'\\(\\d*\\)', u''),  # (123213) -> ''
    (u'#+', u''),  # ### ->
    (u'\\[\\[\\s*(.*?)\\s*\\]\\]', u'\\1'),  # [[ wfaf ]] -> wfaf
    (u"'''(.*?)'''\s*([.,:]?)", u"\\1\\2"),  # '''...''' -> ...
    (u"''(.*?)''\s*([.,:]?)", u""),  # ''..'' ->
    (u'[}{]+\s*', u''),

    # pretty clean
    (u'[(](\s*)[)]', u""),  # (  ) ->
    (u'[ ]+', u' '),  # remove multi spaces
    (u'[ ]+([,.:;)])', u'\\1'),  # remove spaces
    (u'^[,.:;)}-· ]+', u''),  # remove begin
    (u'[,.:;({-· ]+$', u''),  # remove end
]

FIGURE_BRACKETS_EXCEPTIONS = {
    u'итп': (u' и т. п.', True),  # '{{итп}}'
    u'-': (u' - ', False),  # '{{-}}'
    u'музы': (u'музы', True),  # '{{музы}}'
    u'много': (u'много', True),  # '{{музы}}'
}

FIGURE_BRACKETS_LAST_PIPE_EXCEPTIONS = {
    u'': (u'', True),
    u'ru': (u'', True),
}


def add_local_file(sc, filename):
    yarn_xml_file = os.path.join(os.path.dirname(__file__), filename)
    yarn_xml_file = yarn_xml_file.replace('\\', '/')
    sc.addFile(yarn_xml_file)


def figure_brackets_processor(text):
    def processor(match):
        inner = match[2:].strip('.,')[:-2]
        end = inner[-1] if match[-1] in '.,' else ''
        if '{{' in inner:
            inner = figure_brackets_processor(inner)

        if '|' not in inner:
            if inner in FIGURE_BRACKETS_EXCEPTIONS:
                rez, use_end = FIGURE_BRACKETS_EXCEPTIONS[inner]
                return rez + end if use_end else rez
            return ''

        args = inner.split('|')

        if args[-1] == '':
            return ''
        if args[0] == u'помета':
            return ''
        if args[-1] == 'ru':
            return ''

        return args[-1] + end

    brackets = find_nested(text, r'{{', r'}}[,.]?', re.UNICODE)
    start = 0
    buf = []
    for s, e in brackets:
        buf.append(text[start:s])
        buf.append(processor(text[s:e]))
        start = e
    buf.append(text[start:])

    return ''.join(buf)


def html_tags_processor(text):
    soup = BeautifulSoup(text)
    if soup.em:
        soup.em.string = ''  # clean <em> ** </em>
    text_parts = soup.findAll(text=True)
    text = ''.join(text_parts)
    return text


def unescape_html(text):
    return unescape_html._parser.unescape(text)


unescape_html._parser = HTMLParser.HTMLParser()


def _cleaning(text):
    """
    unicode -> unicode
    """
    if not text or text.isdigit():
        return text

    text = unescape_html(text)
    text = html_tags_processor(text)
    text = figure_brackets_processor(text)

    for r, v in FIXES:
        text = re.sub(r, v, text, flags=re.UNICODE)

    text = text.strip(' .')

    # if contain cyrillic symbol -> sentence
    if len(text) > 1 and re.search(u'[а-яА-ЯёЁ]', text):
        rez = text[0].upper() + text[1:] + '.'
    else:
        rez = text

    return rez


def cleaning_xml_line(line):
    """
    str (utf-8) -> cleaned str (utf-8)
    """
    # if type(line) != unicode:
    #     line = line.decode('utf-8')

    soap = BeautifulSoup(line, fromEncoding='utf-8')
    if soap.definition:
        soap.definition.string = _cleaning(soap.definition.string)
    if soap.example:
        soap.example.string = _cleaning(soap.example.string)
        if soap.example.get('source'):
            soap.example['source'] = _cleaning(soap.example['source'])

    return str(soap)


def to_csv_string(data):
    si = StringIO()
    cw = csv.writer(si, quoting=csv.QUOTE_MINIMAL)
    cw.writerow(data)
    return si.getvalue().strip('\r\n')


def from_csv_string(string):
    string = string.strip('\r\n')
    rez = next(iter(csv.reader([string])))
    return rez


f_xml = lambda x: cleaning_xml_line(x) if RE_XML_DEF.search(x) or RE_XML_EX.search(x) else x
f_csv = lambda x: to_csv_string([_cleaning(x.decode('utf-8')).encode('utf-8') for x in from_csv_string(x)])
FORMAT_PROCESSORS = {
    '.xml': f_xml,
    '.csv': f_csv,
}


def get_line_processor(file_path):
    for ext, f in FORMAT_PROCESSORS.items():
        if file_path.endswith(ext):
            return f
    raise Exception('Unknown file extension. Support: {0}'.format(
        ', '.join(FORMAT_PROCESSORS.keys())))


def main(sc, file_path):
    line_processor = get_line_processor(file_path)
    lines = sc.textFile(file_path, 1)
    output = lines.map(line_processor)
    output = output.collect()
    for x in output:
        print(x)


def local_main(file_path):
    line_processor = get_line_processor(file_path)
    with open(file_path, 'r') as file_:
        i = iter(file_)
        i = imap(line_processor, i)
        for x in i:
            yield x + '\n'


# A matching function for nested expressions, e.g. namespaces and tables.
def find_nested(text, open_re, close_re, flags=0):
    """
    Example:
        >>> find_nested(u'{1{11} } }} qwfqwfqwfqwf {, 2{ }2}12121', u'{', u'}')
        [(0, 8), (25, 34)]
        >>> find_nested(' { awdawd } {awda} ', r'{', r'}')
        [(1, 11), (12, 18)]
        >>> find_nested('{}{}', r'{', r'}')
        [(0, 2), (2, 4)]
        >>> find_nested(u'{{помета|обычно{{мн.}}}} затруднение, препятствие, невзгода', r'{{', r'}}')
        [(0, 38)]
        >>> find_nested('{{}}', r'{', r'}')
        [(0, 4)]
    """
    openRE = re.compile(open_re, flags)
    closeRE = re.compile(close_re, flags)
    # partition text in separate blocks { } { }
    matches = []  # pairs (s, e) for each partition
    nest = 0  # nesting level
    start = openRE.search(text, 0)
    if not start:
        return matches
    end = closeRE.search(text, start.end())
    next = start
    while end:
        next = openRE.search(text, next.end())
        if not next:  # termination
            while nest:  # close all pending
                nest -= 1
                end0 = closeRE.search(text, end.end())
                if end0:
                    end = end0
                else:
                    break
            matches.append((start.start(), end.end()))
            break
        while end.end() <= next.start():
            # { } {
            if nest:
                nest -= 1
                # try closing more
                last = end.end()
                end = closeRE.search(text, end.end())
                if not end:  # unbalanced
                    if matches:
                        span = (matches[0][0], last)
                    else:
                        span = (start.start(), last)
                    matches = [span]
                    break
            else:
                matches.append((start.start(), end.end()))
                # advance start, find next close
                start = next
                end = closeRE.search(text, next.end())
                break  # { }
        if next != start:
            # { { }
            nest += 1

    return matches


def get_out_file_name(file_path):
    file_path_name = file_path.replace('\\', '/').rsplit('/', 1)[-1]
    base_name, ext = file_path_name.rsplit('.', 1)
    out_file_name = base_name + ".cleaned." + ext
    return file_path.replace(file_path_name, out_file_name)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and '--test' in sys.argv:
        import doctest

        doctest.testmod()
        sys.exit()

    if len(sys.argv) == 3 and sys.argv[1] == "--local":
        docs_local = """
        Use: cleaner.py --local *.csv
        """
        file_pattern = sys.argv[2]
        for file_ in glob.glob(file_pattern):
            print("Cleaning: {0}".format(file_))
            out_file_ = get_out_file_name(file_)
            print("Out: {0}".format(out_file_))
            with open(out_file_, "w") as f:
                for x in local_main(file_):
                    f.write(x)
        sys.exit()

    if len(sys.argv) < 3:
        sys.exit("Use: cleaner.py <master> <file>")

    from pyspark import SparkContext
    from pyspark import SparkFiles

    sc = SparkContext(sys.argv[1], "PythonYarnCleaner")
    main(sc, sys.argv[2])


    # z = (set([u'\u043f\u043e\u044d\u0442.', u'\u0438\u0442\u043f', u'\u0444\u0438\u0437.', u'\u043f\u043e\u043b\u0438\u0433\u0440.', u'\u0440\u0435\u043b\u0438\u0433.', u'\u0443\u0441\u0442\u0430\u0440.', u'\u0444\u0438\u043b\u043e\u0441.', u'\u0437\u043e\u043e\u043b.', u'\u043d\u0435\u0438\u0441\u0447.', u'\u0433\u0440\u0430\u043c.', u'\u043a\u043d\u0438\u0436\u043d.', u'\u0433\u0435\u043e\u0433\u0440.', u'\u0442\u0435\u0445\u043d.', u'\u043f\u0440\u0435\u0437\u0440.', u'\u043d\u0435\u043e\u043b.', u'\u043f\u0440\u0435\u043d\u0435\u0431\u0440.', u'\u0438\u0441\u0442\u043e\u0440.', u'\u0436\u0430\u0440\u0433.', u'\u043a\u043e\u043c\u043f.\u0436\u0430\u0440\u0433.', u'\u043c\u0443\u0437.', u'\u0440\u0435\u0434\u043a.', u'\u0438\u0441\u0447.', u'\u0431\u0440\u0430\u043d\u043d.', u'\u0441\u043f\u043e\u0440\u0442.', u'\u043d\u0435\u043e\u0434\u043e\u0431\u0440.', u'\u043a\u0443\u043b\u0438\u043d.', u'\u043e\u0431\u043b.', u'\u0443\u043c\u043b\u0430\u0441\u043a.', u'\u0441\u0435\u043b\u044c\u0441\u043a.', u'\u0440\u0438\u0442\u043e\u0440.', u'\u043c\u043e\u0440\u0441\u043a.', u'\u044d\u0442\u043d\u043e\u0433\u0440.', u'\u043e\u0444\u0438\u0446.', u'\u0435\u0434.', u'\u0448\u0443\u0442\u043b.', u'\u0440\u0435\u0433.', u'\u0448\u043a\u043e\u043b\u044c\u043d.', u'\u0432\u0443\u043b\u044c\u0433.', u'\u043f\u0435\u0440\u0435\u043d.', u'\u0444\u0438\u043d.', u'\u044d\u043a\u043e\u043d.', u'\u043a\u043e\u043c\u043f.', u'\u0443\u043d\u0438\u0447\u0438\u0436.', u'\u0442\u043e\u0440\u0436.', u'\u0432\u044b\u0441\u043e\u043a.', u'\u0443\u043c\u043b\u0430\u0441\u043a', u'\u0446\u0435\u0440\u043a.', u'\u0433\u0435\u043e\u043c\u0435\u0442\u0440.', u'\u0442\u0435\u043a\u0441\u0442.', u'\u043d\u0430\u0440.-\u043f\u043e\u044d\u0442.', u'\u0448\u0430\u0445\u043c.', u'\u043c\u0438\u0444\u043e\u043b.', u'\u043f\u0440\u043e\u0441\u0442.', u'\u043c\u0430\u0442', u'\u043c\u0435\u0434.', u'\u043f.', u'\u0441\u043e\u0431\u0438\u0440.', u'\u044e\u0440.', u'\u0430\u0432\u0442\u043e\u043c\u043e\u0431.', u'\u0441\u0442\u0440\u043e\u0438\u0442.', u'\u0431\u0438\u043e\u043b.', u'\u043a\u0430\u0440\u0442.', u'\u0431\u0440\u0430\u043d.', u'\u044d\u0432\u0444.', u'\u0430\u043d\u0430\u0442.', u'\u043b\u0438\u0442.', u'\u043c\u0430\u0442\u0435\u043c.', u'\u0441\u043f\u0435\u0446.', u'\u0432\u043e\u0435\u043d.', u'\u0444\u0438\u043b\u043e\u043b.', u'\u0431\u043e\u0442\u0430\u043d.', u'\u0443\u043d\u0438\u0447.', u'\u0441\u0442\u0430\u0440\u0438\u043d.', u'\u043f\u0441\u0438\u0445\u043e\u043b.', u'\u043f\u043e\u043b\u0438\u0442.', u'\u0432\u043e\u0435\u043d\u043d.', u'\u0444\u0430\u043c.', u'\u0430\u0440\u0445\u0438\u0442\u0435\u043a\u0442.', u'\u043c\u043d.', u'\u0442\u0435\u0430\u0442\u0440.', u'\u0440\u0430\u0437\u0433.', u'\u043b\u0438\u043d\u0433\u0432.', u'\u0443\u043c\u0435\u043d\u044c\u0448.', u'\u043a\u0440\u0438\u043c.']))
