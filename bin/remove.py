"""
# Remove the given keys from the selected file system dictionary.
"""
from .. import library as libfs
from ...routes import library as libroutes

def main(dirpath, keys):
	directory = libroutes.File.from_path(dirpath).fullpath
	d = libfs.Dictionary.open(directory)

	for k in keys:
		k = k.encode('utf-8')
		del d[k]

if __name__ == '__main__':
	import sys
	cmd, dirpath, *keys = sys.argv
	main(dirpath, keys)

