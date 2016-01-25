"""
&..io.libhttp service support for mounting &.library.Dictionary instances.
"""
import functools
import itertools

from . import library as libfs

from ..routes import library as libroutes
from ..internet import libmedia
from ..io import libhttp

def resolve(cache, root, mime_types, accept):
	"""
	Given a root &libfs.Dictionary whose keys are mime-types and whose
	values are &libfs.Dictionary instances, return the &libroutes.Route
	that best matches the acceptable types and the path.

	This function should be bound to an LRU cache in order to optimize
	access to popular files.
	"""

	mtype = accept.query(*mime_types)
	if mtype is None:
		return

	mtype, position, quality = mtype

	if position.pattern:
		# Pattern match; select the set based on it.
		if position != libmedia.any_type:
			# filter dictionaries.
			types = [x for x in mime_types if x in position]
		else:
			# scan all
			types = mime_types
	else:
		types = [mtype]

	for t in types:
		dictpath = root / t.cotype / t.subtype
		if not dictpath.exists():
			continue

		dictionary = cache.get(dictpath)
		if dictionary is None:
			dictionary = libfs.Dictionary.open(str(dictpath))
			cache[dictpath] = dictionary

		yield (t, dictionary)

def headers(route, mtype):
	return [
		(b'Last-Modified', route.last_modified().select('rfc').encode('ascii')),
		(b'Content-Type', bytes(mtype)),
		(b'Content-Length', str(route.size()).encode('ascii')),
	]

class Paths(object):
	"""
	Filesystem mounts based on MIME type.
	"""

	def __init__(self, root):
		self.root = libroutes.File.from_path(root)

		cotypes = self.root.subnodes()[0]
		subtypes = [cotype.subnodes()[0] for cotype in cotypes]
		self.paths = [x.absolute[-2:] for x in itertools.chain(*subtypes)]
		self.types = tuple([libmedia.Type((x[0], x[1], ())) for x in self.paths])
		self.dictionaries = {}

		global resolve
		self.access = functools.partial(resolve, self.dictionaries)

	def __call__(self, path, query, px, str=str):
		p = str('/'.join(path.points)).encode('utf-8')

		for mtype, d in self.access(self.root, self.types, px.request.media_range):
			if not d.has_key(p):
				continue

			r = d.route(p)
			px.response.result(200, 'OK')
			px.response.add_headers(headers(r, mtype))
			px.read_file_into_output(str(r))

			# Found existing resource with matching MIME type.
			break
		else:
			px.host.error(404, path, query, px, None)
