#EXECUTE: python strip_html.py flag_separate_files read_path write_path
#ARGS: flag_separate_files = true - Sript each file to an output file;
#	   flag_separate_files = false - Throw all output to a single file;
#	   files_path: Path of the reading files.
#	   write_path: Path for the created files.
#
# Se houver erro na importacao de 'lxml', instalar com yum para centOS:
#	   1- sudo yum install -y gcc ruby-devel libxml2 libxml2-devel libxslt libxslt-devel
#			OU
#	   1- sudo pip install -U pip setuptools
#	   2- sudo pip install lxml
#

import lxml
from lxml.html.clean import Cleaner
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

cleaner = Cleaner()
cleaner.javascript = True
cleaner.style = True
cleaner.allow_tags = ['']
cleaner.remove_unknown_tags = False
i=0
if not FLAG_SEPARATE_FILES:
	writer = open(WRITE_PATH+"/file.txt", 'w')
for path, dirs, files in os.walk(READ_PATH):
	for filename in files:
		fullpath = os.path.join(path, filename)
		with open(fullpath, 'r') as f:
			i += 1
			if FLAG_SEPARATE_FILES:
				writer = open(WRITE_PATH+"/file"+str(i)+".txt", 'w')
			data = f.read()
			massive_clean = cleaner.clean_html(data)
			remaining_tags = re.sub('<[^>]*>', '', massive_clean)
			remove_url = re.sub(r'https?:\/\/.*[\r\n]*', '', remaining_tags)
			spaces = re.sub('\s{2,}', '', remove_url)
			final = re.sub('[^a-zA-Z0-9 \n]', ' ', spaces)
			writer.write(final);
			if FLAG_SEPARATE_FILES:
				writer.close()
if not FLAG_SEPARATE_FILES:
	writer.close()
