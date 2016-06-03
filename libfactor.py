"""
Factor support for files, directories, and hash stores.
"""
import sys

from ..development import library as libdev
from ..development import core
from ..routes import library as libroutes

class File(libdev.Sources):
	@staticmethod
	def output(role):
		return None

class Directory(libdev.Sources):
	@staticmethod
	def output(role):
		return None

map = {
	name.lower(): Class
	for name, Class in locals().items()
	if isinstance(Class, type) and issubclass(Class, libdev.Sources)
}

def load(typ, *args, map=map):
	"""
	Load a filesystem factor module.
	"""
	ctx = core.outerlocals()
	module = sys.modules[ctx['__name__']]

	Class = map[typ]
	module.__class__ = Class
	module._init()

del map
