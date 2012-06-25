from engine import *
import os

enginex = engine()

try:
    enginex.load_task("setting.txt")
    enginex.run()
    print "[*] ----- enjoy python by boywhp -----"
    os.system("pause")
except:
    print "please check setting.txt or plugin for error!!!"
    
