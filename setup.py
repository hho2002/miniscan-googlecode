import sys
from cx_Freeze import setup, Executable

#
# http://cx_freeze.readthedocs.org/en/latest/distutils.html#cx-freeze-executable
#

# Dependencies are automatically detected, but it might need fine tuning.
#build_exe_options = {"packages": ["os"], "excludes": ["tkinter"]}
build_exe_options = dict(
    optimize = 2,
    #compressed = True,
    create_shared_zip = False,
    append_script_to_exe = True
)

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None
if sys.platform == "win32":
    base = "Win32GUI"

setup(  name = "main",
        version = "0.1",
        description = "console application!",
        options = {"build_exe": build_exe_options},
        executables = [Executable("engine.py", base = "Console")])
