from .. import library

def test_Index_structure(test):
	s1 = [
		b'entry\n',
		b'\tkey1\n',
		b'\tcontinue\n',
	]

	test/list(library.Index.structure(s1)) == [(b'key1\ncontinue', b'entry')]

	s1 = [
		b'1\n',
		b'entry\n',
		b'\tkey1\n',
		b'\tcontinue\n',

		b'entry2\n',
		b'\tkey!\n',
	]

	istruct = [(b'', b'1'), (b'key1\ncontinue', b'entry'), (b'key!', b'entry2')]
	test/list(library.Index.structure(s1)) == istruct

def test_Index(test):
	idx = library.Index()

	# Entry identifier increment.
	entries = idx.allocate((b'key1', b'key2'), str)
	test/entries == ["1", "2"]

	# Check serialization
	l = []
	idx.store(l.append)
	possibilities = [
		[b'2\n', b'1\n\tkey1\n2\n\tkey2\n'],
		[b'2\n', b'2\n\tkey2\n1\n\tkey1\n'],
	]
	test/possibilities << l

	lidx = library.Index()
	lidx.load([x+b'\n' for x in b''.join(l).split(b'\n')])
	test/lidx.counter == 2
	test/lidx._map << b'key1'
	test/lidx._map << b'key2'

	# Check for filename customization along with continuation after load.
	entries = idx.allocate((b'key3',), lambda x: 'F.' + str(x) + '.exe')
	test/entries == ['F.3.exe']

def test_Hash(test):
	h = library.Hash()
	path = h(b'http://foo.com/some/resource.tar.gz')

	test/sum(map(len, path)) == h.length
	test/len(path) == h.depth

def dictionary_operations(test, d):
	key = d[b'key'] = b'value'
	key2 = d[b'key2'] = b'SDLKVNSDVLKSDNVDSVLDVKNSDLVNKasdvdsv'

	test/d[b'key'] == key
	test/d[b'key2'] == key2

	test/d.get(b'key') == key
	test/d.get(b'nosuchkey') == None

	test/d.has_key(b'no-such-key') == False
	test/d.has_key(b'key') == True
	test/d.has_key(b'key2') == True

	d.clear()
	test/d.has_key(b'key') == False
	d[b'case'] = b'content'

	r = d.route(b'case')
	test/d.has_key(b'case')
	test/r.exists() == True
	del d[b'case']

	test/d.has_key(b'case') == False
	test/d.get(b'case', 444) == 444

	# exceptions
	test/KeyError ^ (lambda: d[b'nosuchkey'])
	test/KeyError ^ (lambda: d.__delitem__(b'nosuchkey'))

def test_Dictionary(test):
	with library.libroutes.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath
		fsd2 = (tmp / 'd2').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)
		dictionary_operations(test, d)

def test_Dictionary_subdictionary(test):
	with library.libroutes.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)

		s1 = d.subdictionary(b"sub-1")
		dictionary_operations(test, s1)
		s2 = d.subdictionary(b"sub-2")
		dictionary_operations(test, s2)

def test_Dictionary_keys(test):
	with library.libroutes.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)

		d[b"key1"] = b"value1"
		d[b"key2"] = b"value2"
		d[b"key3"] = b"value3"
		d[b"key4"] = b"value4"

		test/set(d.keys()) == {b"key1",b"key2",b"key3",b"key4"}

def test_Dictionary_delete(test):
	with library.libroutes.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)

		d[b"key2"] = b"value2"
		d[b"key4"] = b"value4"
		test/set(d.keys()) == {b"key2", b"key4"}

		test/d.has_key(b"key2") == True
		del d[b"key2"]
		test/d.has_key(b"key2") == False
		test/d.has_key(b"key2") == False
		test/KeyError ^ (lambda: d[b"key2"])

		def repeat():
			# delitem on freshly removed key
			del d[b"key2"]
		test/KeyError ^ repeat

		test/set(d.keys()) == {b"key4"}

if __name__ == '__main__':
	from ...development import libtest
	import sys; libtest.execute(sys.modules['__main__'])
