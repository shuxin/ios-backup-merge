import os
import shutil
import sqlite3
import hashlib
import biplist

def migrate_sms_and_attachment(source,target):

    d_s = False
    d_t = False

    mbdb_s = os.path.join(source, 'Manifest.mbdb')
    if not os.path.exists(mbdb_s):
        mbdb_s = os.path.join(source, 'Manifest.db')

    mbdb_t = os.path.join(target, 'Manifest.mbdb')
    if not os.path.exists(mbdb_t):
        mbdb_t = os.path.join(target, 'Manifest.db')

    smsdb_s = os.path.join(source, "3d0d7e5fb2ce288813306e4d4636395e047a3d28")
    if not os.path.exists(smsdb_s):
        d_s = True
        smsdb_s = os.path.join(source, "3d", "3d0d7e5fb2ce288813306e4d4636395e047a3d28")

    smsdb_t = os.path.join(target, "3d0d7e5fb2ce288813306e4d4636395e047a3d28")
    if not os.path.exists(smsdb_t):
        d_t = True
        smsdb_t = os.path.join(target, "3d", "3d0d7e5fb2ce288813306e4d4636395e047a3d28")

    con_fs = sqlite3.connect(mbdb_s)
    con_ft = sqlite3.connect(mbdb_t)
    con_ss = sqlite3.connect(smsdb_s)
    con_st = sqlite3.connect(smsdb_t)
    con_fs.text_factory = str
    con_ft.text_factory = str
    con_ss.text_factory = str
    con_st.text_factory = str

    step_1_message_s = {}# k:uuid v:line
    step_1_message_t = {}# k:uuid v:rowid
    step_1_message_e = {}# k:oldid v:newid
    step_2_attachment_s = {}
    step_2_attachment_t = {}
    step_2_attachment_e = {}
    step_3_att_msg_join_s = {}

    step_8_filelist = []
    step_8_filelist_s = {}
    step_8_filelist_t = {}


    ## write_message, guid

    # read old
    cur_ss = con_ss.cursor()
    cur_ss.execute("SELECT * FROM `message`;")
    for line in cur_ss.fetchall():
        # lline = list(line)
        # # for i in range(0,len(line)):
        # #     if type(line[i]) is buffer:
        # #         lline[i] = lline[i].__str__()
        # step_1_message_s[line[1]] = lline
        step_1_message_s[line[1]] = line
    cur_ss.close()

    # read exist
    cur_st = con_st.cursor()
    cur_st.execute("SELECT * FROM `message`;")
    for line in cur_st.fetchall():
        step_1_message_t[line[1]] = line[0]
    cur_st.close()

    # write new
    maxid = max([0]+step_1_message_t.values())
    cur_st = con_st.cursor()
    for uuid,line in step_1_message_s.items():
        if not step_1_message_t.has_key(uuid):
            maxid += 1
            oldid = line[0]
            lline = list(line)
            lline[0] = maxid
            line = tuple(lline)
            step_1_message_e[oldid] = maxid
            cur_st.execute("INSERT INTO `message` VALUES(" +",".join(["?"] * len(line))+ ");",line)
            if cur_st.rowcount != 1:
                raise
            # print str("INSERT INTO `message` VALUES(" +",".join(["?"] * len(line))+ ");").replace("?","%s") % line
    cur_st.close()

    ## write_atttchment, guid
    cur_ss = con_ss.cursor()
    cur_ss.execute("SELECT * FROM `message_attachment_join`;")
    for msgid,attid in cur_ss.fetchall():
        if step_1_message_e.has_key(msgid):
            step_3_att_msg_join_s[attid] = msgid
    cur_ss.close()

    # read old
    cur_ss = con_ss.cursor()
    cur_ss.execute("SELECT * FROM `attachment`;")
    for line in cur_ss.fetchall():
        if step_3_att_msg_join_s.has_key(line[0]):
            step_2_attachment_s[line[1]] = line
            step_8_filelist.append(line[4])
    cur_ss.close()

    # read exist
    cur_st = con_st.cursor()
    cur_st.execute("SELECT * FROM `attachment`;")
    for line in cur_st.fetchall():
        step_2_attachment_t[line[1]] = [line[0]]
    cur_st.close()

    # write new
    maxid = max([0]+step_2_attachment_t.values())
    cur_st = con_st.cursor()
    for uuid,line in step_2_attachment_s.items():
        if not step_2_attachment_t.has_key(uuid):
            maxid += 1
            oldid = line[0]
            lline = list(line)
            lline[0] = maxid
            line = tuple(lline)
            step_2_attachment_e[oldid] = maxid
            cur_st.execute("INSERT INTO `attachment` VALUES(" +",".join(["?"] * len(line))+ ");",line)
            if cur_st.rowcount != 1:
                raise
            # print str("INSERT INTO `attachment` VALUES(" +",".join(["?"] * len(line))+ ");").replace("?","%s") % line
    cur_st.close()

    ## write_msg_att, join
    cur_st = con_st.cursor()
    for attid,msgid in step_3_att_msg_join_s.items():
        msgid_t = step_1_message_e.get(msgid)
        attid_t = step_2_attachment_e.get(attid)
        if not msgid_t or not attid_t:
            raise
        line = (msgid_t,attid_t)
        cur_st.execute("INSERT INTO `message_attachment_join` VALUES(" +",".join(["?"] * len(line))+ ");",line)
        if cur_st.rowcount != 1:
            raise
        print str("INSERT INTO `message_attachment_join` VALUES(" +",".join(["?"] * len(line))+ ");").replace("?","%s") % line
    cur_st.close()

    # write_chat, chat_id
    # TODO:
    # write_chat_msg, join
    # TODO:
    # wirte_handle, hand_id
    # TODO:
    # write_chat_had, join
    # TODO:


    con_ss.commit()
    con_st.commit()


    # copy attachment file
    cur_fs = con_fs.cursor()
    cur_fs.execute("SELECT * FROM Files WHERE `domain` = 'MediaDomain';")
    for line in cur_fs.fetchall():
        step_8_filelist_s[line[0]] = line
    cur_fs.close()
    #_fileID,_domain,_relativePath,_flags,_file
    cur_ft = con_ft.cursor()
    cur_ft.execute("SELECT * FROM Files WHERE `domain` = 'MediaDomain';")
    for line in cur_ft.fetchall():
        step_8_filelist_t[line[0]] = line
    cur_ft.close()

    cur_ft = con_ft.cursor()
    for filename in step_8_filelist:
        if filename.startswith("~/"):
            filename = filename[2:]
        fileid = hashlib.sha1("MediaDomain" +'-' + filename).hexdigest()
        path_s = os.path.join(source,fileid)
        if d_s:
            path_s = os.path.join(source, fileid[:2], fileid)
        path_t = os.path.join(target,fileid)
        if d_t:
            path_t = os.path.join(target, fileid[:2], fileid)
            if not os.path.exists(os.path.join(target, fileid[:2])):
                os.mkdir(os.path.join(target, fileid[:2]))
        if os.path.exists(path_s):
            shutil.copyfile(path_s,path_t)
        while True:
            fileid = hashlib.sha1("MediaDomain" + '-' + filename).hexdigest()
            if step_8_filelist_s.has_key(fileid) and not step_8_filelist_t.has_key(fileid):
                line = step_8_filelist_s[fileid]
                cur_ft.execute("INSERT INTO `Files` VALUES(" +",".join(["?"] * len(line))+ ");",line)
                if cur_ft.rowcount != 1:
                    raise
                step_8_filelist_t[fileid] = line
                # print str("INSERT INTO `Files` VALUES(" + ",".join(["?"] * len(line)) + ");").replace(
                #     "?", "%s") % line
            filename,_,_ = filename.rpartition("/")
            if not _:
                break
    cur_ft.close()

    con_fs.commit()
    con_ft.commit()
    con_fs.close()
    con_ft.close()

if __name__ == "__main__":
    migrate_sms_and_attachment(
        r'C:\Users\shuxi\AppData\Roaming\Apple Computer\MobileSync\Backup\86eda093383511ae0bfc00c2ef2fb94927884de3',
        r'C:\Users\shuxi\AppData\Roaming\Apple Computer\MobileSync\Backup\86eda093383511ae0bfc00c2ef2fb94927884de3-emp'
    )






