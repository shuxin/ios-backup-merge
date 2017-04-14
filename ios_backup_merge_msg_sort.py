import os
import sys
import sqlite3
import hashlib
import binascii
import struct
import biplist
from cStringIO import StringIO

def sha1(filename):
    h = None
    try:
        with open(filename) as f:
            data = f.read()
            h = hashlib.sha1(data).hexdigest()
    except:
        pass
    return h

def message_sort(filename):
    msg_chat = {}
    msg_date = {}
    msg_attr = {}
    msg_sort = {}
    exchange = {}

    test1 = []
    test2 = []


    con = sqlite3.connect(filename)
    con.text_factory = str

    cur = con.cursor()
    cur.execute("SELECT `chat_id`,`message_id` FROM `chat_message_join`;")
    for _ch,_id in cur.fetchall():
        if msg_chat.has_key(_id):
            raise
        msg_chat[_id] = _ch
    cur.close()

    cur = con.cursor()
    cur.execute("SELECT `ROWID`,`date` FROM `message` ORDER BY `date` ASC, `ROWID` ASC;")
    for _id,_dt in cur.fetchall():
        if msg_date.has_key(_id):
            raise
        msg_date[_id] = _dt
        test1.append(_id)
        # print _id,_dt
    cur.close()

    cur = con.cursor()
    cur.execute("SELECT `message_id`,`attachment_id` FROM `message_attachment_join`;")
    for _id,_at in cur.fetchall():
        if not msg_attr.has_key(_id):
            msg_attr[_id] = []
        msg_attr[_id].append(_at)
    cur.close()


    a = len(msg_chat)
    b = len(msg_date)
    c = len(msg_attr)
    d = len(set(msg_chat.keys()).intersection(msg_date.keys()))

    # print "msg_chat",a
    # print "msg_date",b
    # print "msg_attr",c
    # print "        ",d

    if a == b and a == d:
        emax = max(msg_chat.keys() + msg_date.keys() + msg_attr.keys())
        emin = min(msg_chat.keys() + msg_date.keys() + msg_attr.keys())
        if emin > d:
            e = 0
        else:
            e = emax
        f = 10 ** len(str(emax))
        # print e , f
        for k,v in msg_date.items():
            msg_sort[k] = v * f + k
            # print k,msg_sort[k]
        test2 = sorted(msg_sort.keys(),key=lambda x: msg_sort[x], reverse=False)
        for i in range(0,d):
            if(test1[i]!=test2[i]):
                print test1[i],test2[i],test1[i]==test2[i], msg_date[test1[i]], msg_date[test2[i]]
                raise
        for _id in test1:
            e += 1
            # print _id, e
            exchange[_id] = e

    for _id,_xd in exchange.items():
        # print _id,_xd

        cur = con.cursor()
        cur.execute("UPDATE `message`                     SET `ROWID`      = ? WHERE `ROWID`      = ?;",(_xd,_id))
        a = cur.rowcount
        cur.execute("UPDATE `chat_message_join`           SET `message_id` = ? WHERE `message_id` = ?;",(_xd,_id))
        b = cur.rowcount
        cur.execute("UPDATE `message_attachment_join`     SET `message_id` = ? WHERE `message_id` = ?;",(_xd,_id))
        c = cur.rowcount
        # print a,b,c
        cur.close()
        if a == 1 and b == 1 and len(msg_attr.get(_id,[])) == c:
            pass
        else:
            print a, b, c ,  len(msg_attr.get(_id,[]))
            raise

    cur = con.cursor()
    cur.execute("UPDATE `sqlite_sequence` SET seq = ? WHERE `name` = 'message';", (e,))
    cur = con.cursor()
    con.commit()
    con.close()
    print d, e
    return d == e

# def fix_size_and_hash(mbdb,hash0,size1,hash1,size2,hash2):
def fix_size_and_hash(mbdb,hash0,size2,hash2):
    print hash0,size2,hash2
    con = sqlite3.connect(mbdb)
    con.text_factory = str
    cur = con.cursor()
    cur.execute("SELECT file FROM Files WHERE fileID = ?;", (hash0,))
    for _file, in cur.fetchall():
        buff = _file.__str__()
        # print repr(buff)
        bpl = biplist.PlistReader(StringIO(buff))
        bp = bpl.parse()
        sizex = ord(buff[bpl.size_offset]) & 0x0f
        size1 = buff[bpl.size_offset + 1:bpl.size_offset + 1 + pow(2, sizex)]
        ss = {
            0: '!B',
            1: '!H',
            2: '!I',
            4: '!Q',
        }.get(sizex, "!I")
        size2 = struct.pack(ss, size2)
        hash1 = bp["$objects"][3]
        hash2 = binascii.a2b_hex(hash2)
        if size1 != size2:
            buff = buff[:bpl.size_offset + 1] + size2 + buff[bpl.size_offset + 1 + len(size2):]
        if hash1 != hash2:
            buff = buff.replace(hash1,hash2)
        # print [size1,size2]
        # print [hash1,hash2]
        # print biplist.PlistReader(StringIO(buff)).parse()
        # print repr(buff)
        # print repr(bp["$objects"][3]),bp["$objects"][1]["Size"], bp["$objects"][2]
        # print [binascii.b2a_hex(bp["$objects"][3]),bp["$objects"][1]["Size"], bp["$objects"][2]]
        # print [hash2,size2]
        cur.execute("UPDATE Files SET file = ? WHERE fileID = ?;",(buffer(buff),hash0))
    cur.close()
    con.commit()
    con.close()
    return None


if __name__ == '__main__':
    backup = r"""C:\Users\shuxi\AppData\Roaming\Apple Computer\MobileSync\Backup\37ce3e3bf4daa80643f84db462cfe346bcfa6f55-ip5-20151117"""
    backup = r"""C:\Users\shuxi\AppData\Roaming\Apple Computer\MobileSync\Backup\86eda093383511ae0bfc00c2ef2fb94927884de3"""
    if len(sys.argv) > 1:
        backup = sys.argv[1]

    hash0 = "3d0d7e5fb2ce288813306e4d4636395e047a3d28"
    filename = os.path.join(backup, hash0)
    if not os.path.exists(filename):
        filename = os.path.join(backup, hash0[0:2], hash0)
        if not os.path.exists(filename):
            print filename
            raise

    # size1 = os.path.getsize(filename)
    # hash1 = sha1(filename)

    while True:
        if message_sort(filename):
            break

    size2 = os.path.getsize(filename)
    hash2 = sha1(filename)

    mbdb = os.path.join(backup, 'Manifest.db')

    # fix_size_and_hash(mbdb, hash0, size1, hash1, size2, hash2)
    fix_size_and_hash(mbdb, hash0, size2, hash2)
