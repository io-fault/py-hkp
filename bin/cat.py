"""
# Cat a series of entries from a given hash directory.
"""
from .. import library
from ...system import files

def main(output, args, readsize=1024*4):
	directory, *keys = args
	directory = files.Path.from_path(directory).fullpath
	d = library.Dictionary.open(directory)
	write = output.write

	for x in keys:
		k = x.encode('utf-8', 'surrogateescape')
		if not d.has_key(k):
			continue
		r = d.route(k)
		with r.fs_open('rb') as f:
			rs = readsize
			read = f.read
			while rs == readsize:
				data = read(readsize)
				write(data)
				rs = len(data)

if __name__ == '__main__':
	import sys
	try:
		main(sys.stdout.buffer, sys.argv[1:])
	finally:
		sys.stdout.close()
