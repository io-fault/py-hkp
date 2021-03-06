"""
# Put the data given to standard input into the filesystem dictionary.
"""
from .. import library

from ...system import files

def main(input, args, readsize=1024*4):
	directory, key = args
	directory = files.Path.from_path(directory).fullpath
	d = library.Dictionary.open(directory)
	read = input.read

	r = d.route(key.encode('utf-8', 'surrogateescape'))
	with r.fs_open('wb') as f:
		rs = readsize
		write = f.write
		while rs == readsize:
			data = read(readsize)
			write(data)
			rs = len(data)

if __name__ == '__main__':
	import sys
	main(sys.stdin.buffer, sys.argv[1:])
