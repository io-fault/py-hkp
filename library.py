"""
# Provides a dictionary interface for storing files using arbitrary keys.

# The storage abstractions do not provide any guarantees for concurrent
# access. Applications must identify the necessary exclusion constraints.
"""
import hashlib
import functools
import collections.abc

from ..system import files
from ..routes import types as routes

class FNV(object):
	"""
	# Temporary location for simple hash implementation.
	"""
	@classmethod
	def compute(Class, data:bytes):
		c=Class()
		c.update(data)
		return c

	def __init__(self, I=0xcbf29ce484222325):
		self.state = I

	def update(self, data:bytes, P=0x100000001b3, I=0xcbf29ce484222325, C=(2**64)-1):
		s = self.state
		for x in data:
			s ^= x
			s *= P
		self.state = s & C

		return self

	def hexdigest(self):
		return hex(self.state)[2:]

class Hash(object):
	"""
	# Hash algorithm manager providing access to the divided hash of a key.
	# The divided hash is used to construct the route (path) to the actual data file
	# and the index of the bucket.

	# Hash instances are used to manage address resolution for &Dictionary objects.
	"""

	@staticmethod
	def divide(digest, length, edge, step):
		"""
		# Split the hex digest into parts for building a Route to the bucket.
		"""
		return [
			digest[x:y]
			for x, y in zip(
				range(0, length, step),
				range(step, edge, step)
			)
		]

	def __call__(self, key):
		"""
		# Hash the given key with the configured algorithm returning the divided digest.
		"""

		hi = self.implementation(key)
		digest = hi.hexdigest().lower()

		return self.divide(digest, self.length, self.edge, self.step)

	def __init__(self, algorithm='fnv1a_64', depth=2, length=None):
		"""
		# Initialize a &Hash instance according to the parameters.

		# Essentially, a high-level &functools.partial constructor for performing
		# hashes on small keys and returning a tuple suitable for directory
		# names.
		"""
		self.algorithm = algorithm
		self.depth = depth

		if algorithm == 'fnv1a_64':
			self.implementation = FNV.compute
		else:
			self.implementation = hashlib.__dict__[algorithm]

		# calculate if not specified.
		if length is None:
			self.length = len(self.implementation(b'').hexdigest())
		else:
			self.length = length

		self.step = self.length // self.depth
		self.edge = self.length + self.step

class Index(object):
	"""
	# A bucket index for the filesystem &Dictionary.

	# Manages the sequence of entries for a bucket.

	# The index files are a series of entry identifiers followed
	# by the key on a greater indentation level; the trailing newline
	# of each indentation block not being part of the key.
	"""

	@staticmethod
	def structure(seq, iter=iter, next=next, tab=b'\t'[0]):
		"""
		# Structure the indentation blocks into (key, entry) pairs.
		# The keys are the indented section and the entries are the leading
		# unindented identifier.

		# Trailing newlines *MUST* be present.
		# Structure presumes that the index file was loaded using readlines.

		# Entries (unindented areas) must be a single line followed by one
		# or more indented lines. The initial indentation level (first tab)
		# will be remove; the content will be considered to be the continuation
		# of the key (that's hashed to select this bucket's index).

		# Underscore attributes are representations of stored data.
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
		# Load the index from the given line sequence.
		# &lines items *must* have trailing newlines.
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

	def store(self, write:collections.abc.Callable,
			indent = lambda x: b''.join((b'\t', b'\n\t'.join(x.split(b'\n')), b'\n'))
		):
		"""
		# Send the serialized index state to the given &write function.
		"""

		entries = b''.join((v.encode('utf-8')+b'\n'+indent(k)) for (k, v) in self._map.items())
		write(str(self.counter).encode('utf-8') + b'\n')
		write(entries)

	def keys(self):
		"""
		# Iterator containing the keys loaded from the index.
		"""

		return self._map.keys()

	def items(self):
		return self._map.items()

	def has_key(self, key):
		"""
		# Check if a key exists in the index.
		"""

		return key in self._map

	def __getitem__(self, key):
		return self._map[key]

	def __delitem__(self, key):
		del self._map[key]

	def allocate(self, keys, filename):
		"""
		# Allocate a sequence of entries for the given keys.
		"""

		return [
			self._map[k] if k in self._map
			else self.insert(k, filename)
			for k in keys
		]

	def insert(self, key, filename):
		"""
		# Insert the key into the bucket. The key *must* not already be present.
		"""
		self.counter = c = self.counter + 1
		r = self._map[key] = filename(c)

		return r

	def delete(self, key):
		"""
		# Delete the key from the index returning the entry for removal.
		"""

		entry = self._map.pop(key)
		return entry

class Dictionary(collections.abc.Mapping):
	"""
	# Filesystem based dictionary for large values.

	# Used to store files addressed by arbitrary keys.
	# The mapping interface may be used, but it will not be [memory] efficient for larger
	# files.

	# The dictionary interface is provided for convenience and testing.
	# The &allocate method is the primary interface as &Dictionary objects
	# are intended for file storage; large binary objects.

	# [ Properties ]

	# /addressing/
		# The address resolution method. Usually a &Hash instance.

	# /directory/
		# The &routes.Selector instance selecting the directory that the addresses
		# exists within.
	"""

	@staticmethod
	def _init(a, d):
		# Initialize the directory and create the hash configuration file.
		d.init('directory')
		h = d / 'hash'
		data = "%s %s %s\n" %(a.algorithm, a.depth, a.length)
		h.store(data.encode('ascii'))

	@classmethod
	def create(Class, addressing:Hash, directory:str) -> "Dictionary":
		"""
		# Create the Dictionary directory and initialize its configuration.

		# [ Parameters ]

		# /addressing/
			# A &Hash instance describing the to use.
		# /directory/
			# The absolute path to the storage location.
		"""

		r = files.Path.from_path(directory)
		a = addressing
		Class._init(a, r)

		return Class(a, r)

	@classmethod
	def open(Class, directory:str) -> "Dictionary":
		"""
		# Open a filesystem based dictionary at the given directory.

		# [ Parameters ]

		# /directory/
			# An absolute path to the storage location.
		"""

		r = files.Path.from_path(directory)
		config = (r / 'hash').load().decode('ascii')

		algorithm, divisions, *length = config.strip().split(' ', 3) # expecting two fields
		if not length or length[0] == 'None':
			length = None
		else:
			length = int(length[0], 10)
		addressing = Hash(algorithm, depth=int(divisions), length=length)

		return Class(addressing, r)

	@classmethod
	def use(Class, route:routes.Selector, addressing=None):
		"""
		# Create or Open a filesystem &Dictionary at the given &route.
		"""
		if route.exists() and (route / 'hash').exists():
			return Class.open(str(route))
		else:
			return Class.create(addressing or Hash('fnv1a_64'), str(route))

	def __init__(self, addressing:Hash, directory:routes.Selector):
		self.addressing = addressing
		self.directory = directory

	def keys(self) -> [bytes]:
		"""
		# Returns an iterator to all the keys contained in the &Dictionary.
		# The implementation is indifferent to depth and walks the tree looking
		# for index files in order to extract the keys.
		"""

		q = [self.directory]
		while q:
			fsdir = q.pop(0)

			nodes = fsdir.subnodes()
			for x in nodes[0]:
				idx_path = x / 'index'
				if idx_path.exists():
					yield from self._index(idx_path).keys()
				else:
					# container, descend if &x/index does not exist.
					q.append(x)

	def values(self) -> [bytes]:
		"""
		# Return an iterator to the file contents of each key.

		# ! NOTE:
			# This method is intentionally left inefficient as it is
			# unlikely to receive direct use. Its use is likely reasonable in
			# cases of many small files, which is not an intended use
			# case of &Dictionary.
		"""
		yield from (self[k] for k in self.keys())

	def references(self) -> [(bytes, routes.Selector)]:
		"""
		# Returns an iterator to all the keys and their associated routes.
		"""

		q = [self.directory]
		while q:
			fsdir = q.pop(0)

			nodes = fsdir.subnodes()
			for x in nodes[0]:
				idx_path = x / 'index'
				if idx_path.exists():
					yield from (
						(k, (x / v))
						for k, v in self._index(idx_path).items()
					)
				else:
					# container, descend if &x/index does not exist.
					q.append(x)

	def has_key(self, key):
		"""
		# Whether or not the &key is in the dictionary.

		# Notably, the entry file must exist on the file system
		# as well as the &key within the index.
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

	def __len__(self):
		return NotImplemented

	def __iter__(self):
		return NotImplemented

	def __hash__(self):
		return id(self)

	@functools.lru_cache(32)
	def _index(self, route):
		idx = Index()

		with route.open('rb') as f:
			idx.load(f.readlines())

		return idx

	def refresh(self, route):
		"""
		# Refresh the index for the given route.

		# Used when there is suspicion of concurrent writes to a bucket index.
		"""

		idx = self._index(route)
		with route.open('rb') as f:
			idx.load(f.readlines())

	def allocate(self, keys, filename=str):
		"""
		# Allocate a set of keys and return a mapping of their corresponding entries.

		#!/pl/python
			fsd.allocate([(b'/file-1', b'/file-2')])
			m = {
				b'/file-1': fault.system.files.Path(...),
				b'/file-2': fault.system.files.Path(...)
			}

		# The routes are fully initialized; entries exist in the index,
		# and the route points to an initialized file.
		"""

		return {k: self.route(key, filename=filename) for k in keys}

	def usage(self):
		"""
		# Calculate the stored memory usage of the resources.
		"""

		return self.directory.usage()

	def route(self, key, filename=str):
		"""
		# Return the route to the file of the given key.
		"""

		path = self.addressing(key)
		r = self.directory.extend(path)

		ir = r / 'index'
		if not ir.exists():
			ir.init('file')

		# update the index
		idx = self._index(ir)

		entry = idx.allocate((key,), filename=filename)[0]
		with ir.open('wb') as f:
			idx.store(f.write)

		return r / entry

	def subdictionary(self, key, addressing = None):
		"""
		# Create or open a &Dictionary instance at the given key.

		# The addressing (hash and depth) of the subdictionary is
		# consistent with the container's.
		"""

		r = self.route(key)
		if not r.exists():
			r.init("directory")

		return self.__class__(addressing or self.addressing, r)

	def __setitem__(self, key, value):
		"""
		# Store the given data, &value, using the &key.
		"""

		with self.route(key).open('wb') as f:
			f.write(value)

	def __getitem__(self, key):
		"""
		# Get the data stored using the give &key. &KeyError if it does not exist.
		"""

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
		"""
		# Delete the file associated with the key.
		"""

		# Get bucket.
		path = self.addressing(key)
		r = self.directory.extend(path)
		ir = r / 'index'
		if not ir.exists():
			raise KeyError(key)

		# Resolve entry from bucket.
		idx = self._index(ir)
		if not idx.has_key(key):
			raise KeyError(key)

		# Remove key from index.
		entry = idx[key]
		idx.delete(key)
		with ir.open('wb') as f:
			idx.store(f.write)

		# Remove file.
		er = r / entry
		if er.exists():
			er.void()
		else:
			raise KeyError(key)

	def clear(self):
		"""
		# Remove the entire directory and create a new one with the same configuration.
		"""

		self.directory.void()
		self._init(self.addressing, self.directory)

	def update(self, iterable):
		for k, v in iterable:
			r = self.route(k)
			with r.open('wb') as f:
				f.write(v)

	def merge(self, source):
		"""
		# Not Implemented.

		# Merge the &source Dictionary into the hash managed by &self.
		# Merge is fundamental to implementing transactions: A separate hash directory
		# is created to represent a transaction and all writes are made to that
		# temporary directory.
		"""
		raise NotImplementedError("merge")
