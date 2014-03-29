# -*- coding: gb2312 -*-
import sys, re

if __name__ == "__main__":
    '''
        usage:tools log_file filter{field1=value,field2=value}  out{filed1,field2} [split]option
    '''
    
    filename = sys.argv[1]
    outers = sys.argv[3].split(',')
    split_c = '\t'
           
    if len(sys.argv) > 4:
        split_c = sys.argv[4]
    
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

            print s