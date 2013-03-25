#!/usr/bin/python

import xlrd
import simplejson

t = xlrd.open_workbook("/home/atlefren/Documents/navnetype.xls")
s = t.sheet_by_index(0)

print "!!", s.nrows

arr = []
for row_number in range(0, s.nrows-1):
    a = s.row(row_number)
    if a[0].ctype != 1:
        print a[0]
        arr.append({"key": a[0].value, "name": a[1].value.strip(), "desc": a[2].value.strip()})
f = open('navnetype.json', 'w')
f.write(simplejson.dumps(arr))
f.close()