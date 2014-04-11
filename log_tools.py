# -*- coding: gb2312 -*-
import sys, re

strip_set = set()

def process_line(line, filters, outers, strip):
    fileds = line_txt.split(split_c)
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
            if outer != outers[len(outers) - 1]:
                s += '\t'

        if strip:
            if not s in strip_set:
                strip_set.add(s)
                print s
        else:
            print s

if __name__ == "__main__":
    '''
        usage:tools log_file filter{field1=value,field2=value}  out{filed1,field2} [option]
    '''
    filename = sys.argv[1]
    
    split_c = '\t'
    options = {'split':'\t', 'strip':0, 'line_split':''}
    
    if len(sys.argv) > 4:
        for opt in sys.argv[4].split(','):
            key,value = opt.split('=')
            options[key] = value
    
    split_c = options['split']
    strip = int(options['strip'])
    line_split = options['line_split']
    
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

    fp = open(filename, 'rb')
    line_txt = ''
    for line in fp:   
        if not line_split in line:
            line_txt += line
            continue
        
        if line_txt:
            print line_txt
            process_line(line_txt, filters, outers, strip)
            
        line_txt = line
    
    #EOF
    process_line(line_txt, filters, outers, strip)

