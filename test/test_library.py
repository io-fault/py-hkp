from .. import library

def test_feature(test):
	test/library.function() == 'value'

if __name__ == '__main__':
	from ...development import libtest
	import sys; libtest.execute(sys.modules['__main__'])
