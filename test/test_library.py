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
	entries = idx.allocate((b'key1', b'key2'))
	test/entries == ["1", "2"]

	# check serialization
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

def test_Hash(test):
	h = library.Hash()
	path = h(b'http://foo.com/some/resource.tar.gz')

	test/sum(map(len, path)) == h.length
	test/len(path) == h.depth

def dictionary_operations(test, d):
	foo = d[b'foo'] = b'bar'
	bar = d[b'bar'] = b'SDLKVNSDVLKSDNVDSVLDVKNSDLVNKasdvdsv'

	test/d[b'foo'] == foo
	test/d[b'bar'] == bar

	test/d.get(b'foo') == foo
	test/d.get(b'nosuchkey') == None

	test/d.has_key(b'bleh') == False
	test/d.has_key(b'foo') == True
	test/d.has_key(b'bar') == True

	d.clear()
	test/d.has_key(b'foo') == False
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
	with library.routeslib.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath
		fsd2 = (tmp / 'd2').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)
		dictionary_operations(test, d)

def test_Dictionary_subdictionary(test):
	with library.routeslib.File.temporary() as tmp:
		fsd1 = (tmp / 'd1').fullpath

		d = library.Dictionary.create(library.Hash(), fsd1)

		s1 = d.subdictionary(b"sub-1")
		dictionary_operations(test, s1)
		s2 = d.subdictionary(b"sub-2")
		dictionary_operations(test, s2)

if __name__ == '__main__':
	from ...development import libtest
	import sys; libtest.execute(sys.modules['__main__'])
