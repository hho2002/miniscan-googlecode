# -*- coding: gb2312 -*-
import sys, re
import hashlib

def strip_same(filename):
    
    fp = open(filename, 'r')
    strip_dict = {}
    for line in fp:
        md5 = hashlib.new("md5", line).hexdigest()
        if strip_dict.has_key(md5):
            continue
        strip_dict[md5] = 1
        print line
            
if __name__ == "__main__":
    '''
        usage:tools log_file filter{field1=value,field2=value}  out{filed1,field2} [option]
    '''
    filename = sys.argv[1]
    
    split_c = '\t'
    options = {'split':'\t', 'strip':0}
    if len(sys.argv) > 4:
        for opt in sys.argv[4].split(','):
            key,value = opt.split('=')
            options[key] = value
    
    strip_dict = {}
    split_c = options['split']
    strip = int(options['strip'])
    outers = sys.argv[3].split(',')
    #filter = [(index, value,  op[=?],), ]
    filters = []
    for _filter in sys.argv[2].split(','):        
        m = re.match(r"(\d+)\s*([=?!]+)\s*(.+)$", _filter)
        if not m:
            raise "filter error!"
        
        id, op, value = m.groups()        
        id = int(id)
        if id < 1 or id > 64 or len(op) > 2:
            raise "filter index error!!!"
        
        filters.append((id, value, op))

    fp = open(filename, 'r')
    for line in fp:
        fileds = line.split(split_c)
        match = 0
        for _filter in filters:
            id, value, op = _filter
            if id > len(fileds):
                break
            
            v1 = fileds[id - 1].strip()
            v2 = value.strip()
            
            if op == '=' and v1 == v2:
                match += 1
            elif op == '?' and v1 and (v2 in v1 or v1 in v2):
                match += 1
            elif op == '!=' and v1 != v2:
                match += 1
            elif op == '!?' and v1 and not (v2 in v1 or v1 in v2):
                match += 1
                
        if match == len(filters):
            s = ""
            for outer in outers:
                s += fileds[int(outer) - 1].strip()
                if outer != outers[len(outers)-1]:
                    s += '\t'

            if strip:
                md5 = hashlib.new("md5", s).hexdigest()
                if not strip_dict.has_key(md5):
                    strip_dict[md5] = 1
                    print s
            else:
                print s
