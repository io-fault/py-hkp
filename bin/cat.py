"""
Cat a series of entries from a given hash directory.
"""
from .. import library
from ...routes import library as routeslib

def main(output, args, readsize=1024*4):
	directory, *keys = args
	directory = routeslib.File.from_path(directory).fullpath
	d = library.Dictionary.open(directory)
	write = output.write

	for x in keys:
		r = d.route(x.encode('utf-8', 'surrogateescape'))
		with r.open('rb') as f:
			rs = readsize
			read = f.read
			while rs == readsize:
				data = read(readsize)
				write(data)
				rs = len(data)

if __name__ == '__main__':
	import sys
	main(sys.stdout.buffer, sys.argv[1:])
