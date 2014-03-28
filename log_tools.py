# -*- coding: gb2312 -*-
import sys, re

if __name__ == "__main__":
    '''
        usage:tools log_file field1=value,field2=value    out[filed1,field2]
    '''
    
    filename = sys.argv[1]
    outers = sys.argv[3].split(',')
    
    #filter = [(index, value,  op[=?],), ]
    filters = []
    for _filter in sys.argv[2].split(','):
        id, value = re.split(r"\s*[=?]\s*", _filter)
        id = int(id)
        if id < 1 or id > 64:
            raise "filter index error!!!"
        filters.append((id, value, '=' in _filter))

    fp = open(filename, 'r')
    for line in fp:
        fileds = line.split('\t')
        match = 0
        for _filter in filters:
            id, value, equal = _filter
            if id > len(fileds):
                break
            if equal and fileds[id - 1].strip() == value.strip():
                match += 1
            elif not equal and value.strip() in fileds[id - 1].strip():
                match += 1
                
        if match == len(filters):
            s = ""
            for outer in outers:
                s += fileds[int(outer) - 1].strip()
                if outer != outers[len(outers)-1]:
                    s += '\t'

            print s