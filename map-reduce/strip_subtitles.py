#EXECUTE: python strip_subtitles.py flag_separate_files read_path write_path
#ARGS: flag_separate_files = true - Sript each file to an output file;
#	   flag_separate_files = false - Throw all output to a single file;
#	   files_path: Path of the reading files.
#	   write_path: Path for the created files.

import re
import sys
import os

WRITE_FILE_NAME = sys.argv[0].split(".")[0]
if len(sys.argv) != 4:
	print "EXEC: python "+WRITE_FILE_NAME+".py 'flag_separate_files' 'read_path' 'write_path'"
	sys.exit()

TRUTH = ["true", "TRUE", "1"]
FLAG_SEPARATE_FILES = sys.argv[1] in TRUTH
READ_PATH = sys.argv[2]
WRITE_PATH = sys.argv[3]

if not os.path.exists(WRITE_PATH):
	os.makedirs(WRITE_PATH)

if not FLAG_SEPARATE_FILES:
	writer = open(WRITE_PATH+"/file.txt", 'w')
for path, dirs, files in os.walk(READ_PATH):
	for filename in files:
		fullpath = os.path.join(path, filename)
		with open(fullpath, 'r') as f:
			if FLAG_SEPARATE_FILES:
				writer = open(WRITE_PATH+"/"+filename, 'w')
			for line in f:
				if ((not (" " in line)) or ("-->" in line)):
					continue
				spaces = re.sub('\s{2,}', '', line)
				final = re.sub('[^a-zA-Z0-9 \n]', ' ', spaces)
				writer.write(final)
			if FLAG_SEPARATE_FILES:
				writer.close()
if not FLAG_SEPARATE_FILES:
	writer.close()
