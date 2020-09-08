import unittest

from shared.corso.examples import simpleDataset

from shared.data.record import genRecordType


class GenerateRecordTypeTestCase(unittest.TestCase):

	def test_instantiation(self):

		# string as a list
		R = genRecordType('abc')
		self.assertEqual(R._fields, ('a', 'b', 'c'))

		# list of columns
		R = genRecordType(['a','b','c'])
		self.assertEqual(R._fields, ('a', 'b', 'c'))

		# from generator
		R = genRecordType((h for h in ['a','b','c']))._fields
		self.assertEqual(R._fields, ('a', 'b', 'c'))

		# from DataSet
		R = genRecordType(simpleDataset)
		self.assertEqual(R._fields, ('a', 'b', 'c'))


		# One columned
		R = genRecordType('a')
		r = R(1)
		self.assertEqual(r._tuple, (1,))


	def test_sanitizingFields(self):

		fields = ['Column 1', '3', 'Col@2!']
		R =  genRecordType(fields)
		self.assertEqual(
			R._lookup, 
			{'3': 1, 'C3': 1, 'Col@2!': 2, 'Col_2_': 2, 'Column 1': 0, 'Column_1': 0} )


	def test_replacement(self):

		R = genRecordType('abc')
		r = R((1,2,3))
		r._replace(b=5)

		self.assertEqual(r._asdict(), {'a': 1, 'b': 5, 'c': 3} )


	def test_interfaces(self):

		# create the example 
		R = genRecordType('abc')
		r = R( (1,2,3) )

		self.assertEqual(r._asdict(), {'a': 1, 'b': 2, 'c': 3} )
		self.assertEqual(r._tuple, (1, 2, 3))
		self.assertEqual(r.values, (1, 2, 3))



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(GenerateRecordTypeTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)
	