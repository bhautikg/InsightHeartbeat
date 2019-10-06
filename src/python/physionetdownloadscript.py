import wfdb
import json
import numpy as np

def saverecord(savedir, recorddir, recordname, channel, recordsabridged):
    """
    This function reads in record signals and metadata using wfdb library (developed by Physionet.com)
    and writes them to disk as separate metadata and signal files.
    :param savedir: directory for saving records
    :param recorddir: directory of records on physionet.com
    :param recordname: name of record
    """
    try:
        signals, fields = wfdb.rdsamp(recordname,channel_names=[channel], pb_dir=recorddir)
         
    except Exception as e:
        # if record is empty, wrong type, etc. catch here and skip record
        print(e.message)
        return None
    recordsabridged.write(recordname+"\n")
    outfileprefix = savedir + '/' + recordname
    np.savetxt(outfileprefix + '_signals.txt', signals, fmt='%.32f')
    metadata(outfileprefix, fields)
    
def parsemghdbcomments(comments):
    """
    Parse age, sex and diagnosis from record comments
    :param comments: comments from record fields
    :return: comments parsed into dictionary
    """
    comment0 = comments[0]
    commentsdict = {}
    if comment0.find('<')==-1:
        #format of header has <> around age, sex and diagnosis fields, this checks for presence of these fields.
        print('Bad header')
        return commentsdict
    #string editing so can split into list
    comment0=comment0.replace('<','*')
    comment0 = comment0.replace('>:', '*')
    comment0 = comment0.split('*')
    for i in range(1,len(comment0)-1, 2):
        commentsdict[comment0[i]] = comment0[i+1].replace(' ', '')
    return commentsdict

def metadata(outfileprefix, fields):
    """
    Save patient and signal metadata from record fields to disk in json format
    :param outfileprefix: outfile name prefix
    :param fields: record metadata
    """
    commentsdict = parsemghdbcomments(fields['comments'])
    fields=dict((k, fields[k]) for k in ['fs', 'n_sig', 'sig_len', 'units', 'sig_name'])
    # combine comments dict and other metadata from record
    newfields = dict(list(fields.items()) + list(commentsdict.items()))
    with open(outfileprefix + '_metadata.txt', 'w') as metadataoutfile:
        json.dump(newfields, metadataoutfile, indent=4, sort_keys=True)


if __name__ == '__main__':
    directory = "./data/mitdb"
    recordsfile = open(directory + "/RECORDS.txt", 'r')
    recordsabridged = open("RECORDS_abridged.txt","a") 
    records = recordsfile.readlines()
    recorddir = 'mitdb'
    channel = 'MLII'
    for i in range(1, len(records)):
        record = records[i]
        print(record)
        recordname = record.replace('\n', '')
        saverecord(directory, recorddir, recordname, recordchan, recordsabridged)
    
    recordsabridged.close()
