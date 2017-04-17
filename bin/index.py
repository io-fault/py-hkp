"""
Emit a newline separated list of keys contained in the hash to standard output.
"""
import itertools
from .. import library
from ...routes import library as libroutes

def main(output, args, str=str):
	write = output.write
	directory, *typ = args
	directory = libroutes.File.from_path(directory).fullpath
	d = library.Dictionary.open(directory)

	if typ and typ[0] == 'xml':
		# Expose this outside of the binary.
		from ...xml import library as libxml
		content = libxml.element('map',
			itertools.chain.from_iterable(
				libxml.element('item',
					libxml.escape_element_string(str(r)),
					('key', k.decode('utf-8'))
				)
				for k, r in d.references()
			),
			('dictionary', directory),
			('xmlns', 'http://fault.io/xml/filesystem#index'),
		)
		sep = b''
	else:
		content = d.keys()
		sep = b'\n'

	write(sep.join(content))

if __name__ == '__main__':
	import sys
	main(sys.stdout.buffer, sys.argv[1:])
