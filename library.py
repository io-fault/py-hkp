"""
Storage abstractions for finite maps and permanent event streams.

The storage abstractions do not provide any guarantees for concurrent
access. Applications must identify the necessary exclusion constraints.
"""

import hashlib
import functools

from ..computation import libhash
from ..routes import library as routeslib

class Hash(object):
	"""
	Hash algorithm manager providing access to the divided hash of a key.
	The divided hash is used to construct the route to the actual data file
	and the index of the bucket.

	Depends on &hashlib for implementation resolution.
	"""

	def __call__(self, key):
		"Hash the given key with the configured algorithm returning the divided digest."

		hi = self.implementation(key)

		digest = hi.hexdigest().lower()
		return [
			digest[x:y]
			for x, y in zip(
				range(0, self.length, self.step),
				range(self.step, self.length+self.step, self.step)
			)
		]

	# XXX: need fast consistent hash here, not crypto hash
	# A crypto hash may be appropriate for file metadata, but not for keys.
	def __init__(self, algorithm='sha256', depth=4):
		self.algorithm = algorithm
		self.depth = depth
		self.implementation = hashlib.__dict__[algorithm]
		self.length = len(self.implementation(b'').hexdigest())
		self.step = self.length // self.depth

class Index(object):
	"""
	A bucket index for the filesystem &Dictionary.

	Manages the sequence of entries for a bucket.

	The index files are a series of entry identifiers followed
	by the key on a greater indentation level; the trailing newline
	of each indentation block not being part of the key.
	"""

	@staticmethod
	def structure(seq, iter=iter, next=next, tab=b'\t'[0]):
		"""
		Structure the indentation blocks into (key, entry) pairs.
		The keys are the indented section and the entries are the leading
		unindented identifier.

		Trailing newlines *MUST* be present.
		Structure presumes that the index file was loaded using readlines.

		Entries (unindented areas) must be a single line followed by one
		or more indented lines. The initial indentation level (first tab)
		will be remove; the content will be considered to be the continuation
		of the key (that's hashed to select this bucket's index).

		Underscore attributes are representations of stored data.
		"""

		if seq:
			si = iter(seq)
			entry = next(si)
			key = b''

			for x in si:
				if x[0] == tab:
					key += x[1:] # any newlines are part of the key
				else:
					# found unindented block, so start new entry
					yield (key[:-1], entry[:-1])
					entry = x
					key = b''
			else:
				yield (key[:-1], entry[:-1])

	def __init__(self):
		self.counter = 0
		self._map = {}
		self._state = []

	def load(self, lines):
		"""
		Load the index from the given line sequence.
		&lines items *must* have trailing newlines.
		"""

		i = iter(self.structure(lines))
		try:
			counter = next(i)[1]
			self.counter = int(counter.decode('utf-8'))
		except StopIteration:
			self.counter = 0

		# remainder are regular values
		self._state = list(i)
		self._map = dict([(k, v.decode('utf-8')) for k, v in self._state])

	def store(self, write,
			indent = lambda x: b''.join((b'\t', b'\n\t'.join(x.split(b'\n')), b'\n'))
		):
		"""
		Send the serialized index state to the write function.
		"""

		entries = b''.join((v.encode('utf-8')+b'\n'+indent(k)) for (k, v) in self._map.items())
		write(str(self.counter).encode('utf-8') + b'\n')
		write(entries)

	def has_key(self, key):
		"Check if a key exists in the index."

		return key in self._map

	def __getitem__(self, key):
		return self._map[key]

	def __delitem__(self, key):
		del self._map[key]

	def allocate(self, keys):
		"Allocate a sequence of entries for the given keys."

		return [
			self._map[k] if k in self._map else self.insert(k)
			for k in keys
		]

	def insert(self, key):
		"""
		Insert the key into the bucket. The key *must* not already be present.
		"""

		self.counter = c = self.counter + 1
		r = self._map[key] = str(c)

		return r

	def delete(self, key):
		"""
		Delete the key from the index returning the entry for removal.
		"""

		entry = self._map.pop(key)
		return entry

class Dictionary(object):
	"""
	Filesystem based dictionary for large values.

	Used to store files addressed by arbitrary keys.
	The mapping interface may be used, but it will not be [memory] efficient for larger
	files.

	/addressing
		The address resolution method. Usually a &Hash instance.

	/directory
		The route to the directory that the address exists within.

	The dictionary interface is provided for convenience and testing.
	The &allocate method is the primary interface as &Dictionary objects
	are intended for file storage; large binary objects.
	"""

	@staticmethod
	def _init(a, d):
		# Initialize the directory and create the hash configuration file.
		d.init('directory')

		with (d / 'hash').open('w') as f:
			f.write("%s %s\n" %(a.algorithm, a.depth))

	@classmethod
	def create(Class, addressing, directory):
		"""
		Create the Dictionary directory and initialize its configuration.

		/addressing
			A &Hash instance describing the to use.
		/directory
			The absolute path to the storage location.
		"""

		r = routeslib.File.from_absolute(directory)
		a = addressing
		Class._init(a, r)

		return Class(a, r)

	@classmethod
	def open(Class, directory):
		"""
		Open a filesystem based dictionary at the given directory.

		/directory
			An absolute path to the storage location.
		"""

		r = routeslib.File.from_absolute(directory)
		with (r / 'hash').open('r') as f: # open expects an existing 'hash' file.
			config = f.read()

		algorithm, divisions = config.strip().split() # expecting two fields
		addressing = Hash(algorithm, depth=divisions)

		return Class(addressing, r)

	def __init__(self, addressing, directory):
		self.addressing = addressing
		self.directory = directory

	def has_key(self, key):
		"""
		Whether or not the &key is in the dictionary.

		Notably, the entry file must exist on the file system
		as well as the &key within the index.
		"""

		path = self.addressing(key)
		r = self.directory.extend(path)
		ir = r / 'index'
		if not ir.exists():
			return False

		idx = self._index(ir)
		if idx.has_key(key):
			entry = idx[key]
			er = r / entry
			if er.exists():
				return True

		return False

	__contains__ = has_key

	@functools.lru_cache(32)
	def _index(self, route):
		idx = Index()

		with route.open('rb') as f:
			idx.load(f.readlines())

		return idx

	def refresh(self, route):
		"""
		Refresh the index for the given route.

		Used when there is suspicion of concurrent writes to a bucket index.
		"""

		idx = self._index(route)
		with route.open('rb') as f:
			idx.load(f.readlines())

	def allocate(self, keys):
		"""
		Allocate a set of keys and return a mapping of their corresponding entries.

		Usage:

			fsd.allocate([(b'/file-1', b'/file-2')])
			{b'/file-1': fault.routes.library.File(),
			 b'/file-2': fault.routes.library.File()}

		The routes are fully initialized; entries exist in the index,
		and the route points to an initialized file.
		"""

		return {k: self.route(key) for k in keys}

	def usage(self):
		"""
		Calculate the stored memory usage of the resources.
		"""

		return self.directory.usage()

	def route(self, key):
		"""
		Return the route to the file of the given key.
		"""

		path = self.addressing(key)
		r = self.directory.extend(path)

		ir = r / 'index'
		if not ir.exists():
			ir.init('file')

		idx = self._index(ir)

		entry = idx.allocate((key,))[0]
		with ir.open('wb') as f:
			idx.store(f.write)

		return r / entry

	def __setitem__(self, key, value):
		"Store the given data, &value, using the &key."

		with self.route(key).open('wb') as f:
			f.write(value)

	def __getitem__(self, key):
		"Get the data stored using the give &key. &KeyError if it does not exist."

		if not self.has_key(key):
			raise KeyError(key)

		with self.route(key).open('rb') as f:
			return f.read()

	def get(self, key, fallback=None):
		if not self.has_key(key):
			return fallback

		with self.route(key).open('rb') as f:
			return f.read()

	def pop(self, key, *fallback):
		if not self.has_key(key):
			if not fallback:
				raise KeyError(key)
			else:
				fb, = fallback
				return fb

		value = self[key]
		del self[key]
		return value

	def __delitem__(self, key):
		"Delete the file associated with the key."

		if self.has_key(key):
			self.route(key).void()
		else:
			raise KeyError(key)

	def clear(self):
		"Remove the entire directory and create a new one with the same configuration."

		self.directory.void()
		self._init(self.addressing, self.directory)

	def update(self, iterable):
		for k, v in iterable:
			r = self.route(k)
			with r.open('wb') as f:
				f.write(v)

	def merge(self, source):
		"""
		Not Implemented.

		Merge the &source Dictionary into the &self.

		The hash configuration must be identical.
		"""
		raise NotImplementedError("merge")
