#!/usr/bin/env python2
# From http://stackoverflow.com/questions/3085153/how-to-parse-the-manifest-mbdb-file-in-an-ios-4-0-itunes-backup
import sys
import hashlib
import base64
import csv
from time import localtime, strftime
from time import mktime, strptime

DEFAULT_NB_RECORD = 15


def getint(data, offset, intsize):
    """Retrieve an integer (big-endian) and new offset from the current offset"""
    value = 0
    while intsize > 0:
        value = (value<<8) + ord(data[offset])
        offset = offset + 1
        intsize = intsize - 1
    return value, offset

def getstring(data, offset):
    """Retrieve a string and new offset from the current offset into the data"""
    if data[offset] == chr(0xFF) and data[offset+1] == chr(0xFF):
        return '', offset+2 # Blank string
    length, offset = getint(data, offset, 2) # 2-byte length
    value = data[offset:offset+length]
    return value, (offset + length)

def process_mbdb_file(filename):
    mbdb = [] # List, we want to keep the same order
    file = open(filename)
    data = file.read()
    if data[0:6] != "mbdb\x05\x00": raise Exception("This does not look like an MBDB file")
    offset = 6
    while offset < len(data):
        fileinfo = {}
        fileinfo['domain'], offset = getstring(data, offset)
        fileinfo['filename'], offset = getstring(data, offset)
        fileinfo['linktarget'], offset = getstring(data, offset)
        fileinfo['datahash'], offset = getstring(data, offset)
        fileinfo['enckey'], offset = getstring(data, offset)
        fileinfo['mode'], offset = getint(data, offset, 2)
        fileinfo['inode'], offset = getint(data, offset, 8)
        fileinfo['userid'], offset = getint(data, offset, 4)
        fileinfo['groupid'], offset = getint(data, offset, 4)
        fileinfo['mtime'], offset = getint(data, offset, 4)
        fileinfo['atime'], offset = getint(data, offset, 4)
        fileinfo['ctime'], offset = getint(data, offset, 4)
        fileinfo['filelen'], offset = getint(data, offset, 8)
        fileinfo['flag'], offset = getint(data, offset, 1)
        fileinfo['numprops'], offset = getint(data, offset, 1)
        fileinfo['properties'] = []
        for ii in range(fileinfo['numprops']):
            propname, offset = getstring(data, offset)
            propval, offset = getstring(data, offset)
            fileinfo['properties'].append([propname, propval])
        fullpath = fileinfo['domain'] + '-' + fileinfo['filename']
        id = hashlib.sha1(fullpath)
        fileinfo['fileID'] = id.hexdigest()
        mbdb.append(fileinfo)
    file.close()
    return mbdb

def modestr(val):
    def mode(val):
        if (val & 0x4): r = 'r'
        else: r = '-'
        if (val & 0x2): w = 'w'
        else: w = '-'
        if (val & 0x1): x = 'x'
        else: x = '-'
        return r+w+x
    return mode(val>>6) + mode((val>>3)) + mode(val)

def convert_time(datecode):
    return strftime('%Y-%m-%d %H:%M:%S (%Z)', localtime(datecode))

def fileinfo_str(f):
    if (f['mode'] & 0xE000) == 0xA000: type = 'l' # symlink
    elif (f['mode'] & 0xE000) == 0x8000: type = '-' # file
    elif (f['mode'] & 0xE000) == 0x4000: type = 'd' # dir
    else:
        print >> sys.stderr, "Unknown file type %04x for %s" % (f['mode'], fileinfo_str(f, False))
        type = '?' # unknown
    info = ("%s%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" %
            (type, modestr(f['mode']&0x0FFF), f['inode'], f['userid'], f['groupid'], f['filelen'],
             convert_time(f['mtime']), convert_time(f['atime']), convert_time(f['ctime']),
             f['fileID'], f['filename'], f['linktarget'], f['domain'], f['flag'],
             base64.b64encode(f['datahash']), base64.b64encode(f['enckey'])))
    for name, value in f['properties']: # extra properties
        info = info + ',' + name + ',' + base64.b64encode(value)
    return info

def writeint(mbdb, int, intsize):
    while intsize > 0:
        intsize -= 1
        mbdb.write("%c" % ((int >> (intsize * 8)) & 0xFF))

def writestring(mbdb, str):
    if not len(str):
        mbdb.write("\xFF\xFF") # Blank string
    else:
        writeint(mbdb, len(str), 2) # 2-byte length
        mbdb.write(str)

def modeval(str):
    def type(char):
        if char == 'l': return 0xA
        elif char == '-': return 0x8
        elif char ==  'd': return 0x4
        else: return '?'
    def mode(char, i):
        # basic position test
        if char == 'r' and not (i + 1) % 3: return 0x1
        elif char == 'w' and not (i + 2) % 3: return 0x1
        elif char == 'x' and not (i + 3) % 3: return 0x1
        else: return 0x0
    modeint = type(str[0]) << 12
    str = str[1:] # slice the type
    i = 8
    for c in str:
        modeint = modeint + (mode(c, i) << i)
        i -= 1
    return modeint

def convert_times(time):
    return int(mktime(strptime(time, '%Y-%m-%d %H:%M:%S (%Z)')))

def parse_row(mbdb, row):
    # First, check for anomality
    filehash = hashlib.sha1(row[11] + '-' + row[9]).hexdigest() # domain-filename
    if row[8] != filehash: # fileID saved in the CSV
        print 'Error, the sha1 of %s returned %s while the CSV saved %s' % (row[9], filehash, row[8])
        return

    writestring(mbdb, row[11]) # domain
    # For some reason, an empty filename is just coded as \x00\x00:
    if len(row[9]):
        writestring(mbdb, row[9])
    else:
        mbdb.write("\x00\x00")
    writestring(mbdb, row[10]) # linktarget
    writestring(mbdb, base64.decodestring(row[13])) # datahash
    writestring(mbdb, base64.decodestring(row[14])) # enckey
    writeint(mbdb, modeval(row[0]), 2) # mode
    writeint(mbdb, int(row[1]), 8) # inode
    writeint(mbdb, int(row[2]), 4) # userid
    writeint(mbdb, int(row[3]), 4) # groupeid
    writeint(mbdb, convert_times(row[5]), 4) # mtime
    writeint(mbdb, convert_times(row[6]), 4) # atime
    writeint(mbdb, convert_times(row[7]), 4) # ctime
    writeint(mbdb, int(row[4]), 8) # filelen
    writeint(mbdb, int(row[12]), 1) # flag

    numprops = (len(row) - DEFAULT_NB_RECORD)
    if numprops % 2:
        print 'ERROR with %s: a property is missing an element (the last value is empty?) or ' \
              'the CSV delimiter is not unique (%s)' % (row[8], numprops)
        return
    numprops /= 2 # props: 1 name, 1 value

    writeint(mbdb, numprops, 1)
    for i in xrange(numprops):
        writestring(mbdb, row[DEFAULT_NB_RECORD + 2 * i]) # name
        writestring(mbdb, base64.decodestring(row[DEFAULT_NB_RECORD + 2 * i + 1])) # value

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: %s In.csv Out.mbdb" % sys.argv[0]
        print "Usage: %s In.mbdb Out.csv" % sys.argv[0]
        sys.exit(2)

    if sys.argv[1].lower().endswith("mbdb"):
        mbdb = process_mbdb_file(sys.argv[1])
        csv = open(sys.argv[2], 'w')
        for fileinfo in mbdb:
            csv.write(fileinfo_str(fileinfo) + '\n')
        csv.close()
    else:
        with open(sys.argv[1], 'rb') as csvfile:
            csvreader =  csv.reader(csvfile)
            mbdb = open(sys.argv[2], 'w')
            mbdb.write('mbdb\x05\x00') # header
            for row in csvreader:
                parse_row(mbdb, row)
            mbdb.close()

