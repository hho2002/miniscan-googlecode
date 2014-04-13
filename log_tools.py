# -*- coding: gb2312 -*-
import sys, re
import hashlib

strip_set = set()
sort_cnt = {}       #key = field[sortid], value=[cnt, [o1, o2, ..]]

def is_string_like(s1, s2):
    s1 = s1.lower()
    s2 = s2.lower()
    return (s1 in s2) or (s2 in s1)

def process_line(line_txt, filters, outers, strip, sortid=0):   
    if '\n' in line_txt:
        line, remain = line_txt.split('\n', 1)
    else:
        line = line_txt
        remain = ''
    
    fileds = line.split(split_c)
    fileds[len(fileds)-1] += remain
    sort_field = None
    
    match = 0
    for _filter in filters:
        id, value, op = _filter
        if id > len(fileds):
            break
            
        v1 = fileds[id - 1].strip()
        v2 = value.strip()
        
        if op == '=' and v1 == v2:
            match += 1
        elif op == '?' and v1 and is_string_like(v1, v2):
            match += 1
        elif op == '!=' and v1 != v2:
            match += 1
        elif op == '!?' and v1 and not is_string_like(v1, v2):
            match += 1
                
    if match == len(filters):
        s = ""
        for outer in outers:
            ofilter = None
            if '=' in outer:
                outer, ofilter = outer.split('=', 1)
            
            t = fileds[int(outer) - 1].strip()                
            if ofilter:
                m = re.search(ofilter, t)
                t = ''
                if m:
                    for g in m.groups(0):
                        t += g
                        if t != m.groups(0)[len(m.groups(0)) - 1]:
                            t += '\t'                   
            
            s += t
            if sortid == int(outer):
                sort_field = t
                
            if outer != outers[len(outers) - 1]:
                s += '\t'

        if strip and s in strip_set:
            return None
        
        if strip:
            strip_set.add(s)
            
        if sort_field:
            if sort_cnt.has_key(sort_field):
                sort_cnt[sort_field][0] += 1
                sort_cnt[sort_field][1].append(s)
            else:
                sort_cnt[sort_field] = [1, [s,]]
                #sort_result[sort_field] = [s, ]

        return s

    return None
    
if __name__ == "__main__":
    '''
        usage:tools log_file filter{field1=value,field2=value}  out{filed1,field2} [option]
    '''
    filename = sys.argv[1]
    
    split_c = '\t'
    options = {'split':'\t', 'strip':0, 'line_split':'', 'sort':0}
    
    if len(sys.argv) > 4:
        for opt in sys.argv[4].split(','):
            key,value = opt.split('=')
            options[key] = value
    
    split_c = options['split']
    strip = int(options['strip'])
    line_split = options['line_split']
    sortid = int(options['sort'])    
    
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
            s = process_line(line_txt, filters, outers, strip, sortid)
            if not sortid and s:
                print s
                
        line_txt = line
    
    #EOF
    s = process_line(line_txt, filters, outers, strip, sortid)
    if not sortid and s:
        print s
    else:
        for x in sorted(sort_cnt.iteritems(), key=lambda d:d[1], reverse=True):
            for s in x[1][1]:
                print s
    
