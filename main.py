# -*- coding: gb2312 -*-
import os, sys
import msvcrt, time
import socket, struct
#import SimpleHTTPServer

MSG_HDR_LEN = struct.calcsize("ii")

CLIENT_CONTROL = 7
CLIENT_ADD_CMD = 8
CLIENT_QUERY_TASK = 9

cmd_list = ["add", "pause", "log", "nodes", "exit"]

def send_msg(sock, cmd, msg):
    stream = struct.pack("ii", cmd, MSG_HDR_LEN + len(msg))
    stream += msg
    sock.send(stream)
    
def __show_help():
    print "Esc to entry cmd! cmd list[add/del pause/run log exit]\n"
    
def update_tasks():
    """ 刷新任务列表状态
    """
    send_msg(client, CLIENT_QUERY_TASK, '')
    msg = client.recv(4096)
    return msg
    #sys.stdout.write("\r")

def show_log(task_name, filename = "result.log"):
    fp = open(filename, 'r')
    log = []
    
    while True:
        buf = fp.readline()
        if not buf:
            break
        item = buf.split('\t', 4)
        log.append(item)

    fp.close()

    for item in sorted(log, key=lambda x:x[2]): # 第三列插件名排序
        s = item[1] + '\t' + item[2]+ '\t' +item[3]
        sys.stdout.write(s)

def add_task(name):
    #print name, client
    f = open(name)
    send_msg(client, CLIENT_ADD_CMD, name + '\0' + f.read())
    f.close()

if __name__ == '__main__':
    client = socket.create_connection(('localhost', 9910))
    
    while True:
        status = "tasks"
        if not msvcrt.kbhit():
            msg = update_tasks()
            if msg:
                os.system('cls')
                __show_help()
                print msg
            time.sleep(1)
            continue
        
        key = msvcrt.getch()
        if key == chr(27):
            cmd = raw_input("\n>>>").split(" ")# msvcrt.getch()
            if cmd[0] == "exit":
                break
            
            if cmd[0] == "add":
                add_task(cmd[1])
                
            if cmd[0] in ("pause", "run", "del"):
                send_msg(client, CLIENT_CONTROL, cmd[0] + '\0' + cmd[1])
                
            if cmd[0] == "log":
                show_log(cmd[1], cmd[1] + '.log')
                print "[*]press any key to back"
                msvcrt.getch()
            
    os.system("pause")
