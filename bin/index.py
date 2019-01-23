"""
# Emit a newline separated list of keys contained in the hash to standard output.
"""
import itertools

from ...system import files

from .. import library

def main(output, args, str=str):
	write = output.write
	directory, *typ = args
	directory = files.Path.from_path(directory).fullpath
	d = library.Dictionary.open(directory)

	content = d.keys()
	sep = b'\n'

	write(sep.join(content))

if __name__ == '__main__':
	import sys
	main(sys.stdout.buffer, sys.argv[1:])
