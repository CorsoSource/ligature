import unittest


from ligature.recordset import RecordSet
from ligature.examples import simpleRecordSet, simpleAddition

from ligature.calculations.window import Window


class WindowTestCase(unittest.TestCase):

	def test_basic(self):

		function = lambda a,b: sum(a) - sum(b)

		srs = RecordSet(simpleRecordSet)
		
		c = Window([srs], function, 'c')

		# Calculations are lazily evaluated
		self.assertEqual(c._resultSet._groups, [])

		# When evaluated, we get the following
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[8, 10, 23]]
			)

		srs.extend(simpleAddition)

		# adding data from a source doesn't immediately update
		self.assertEqual(len(c._resultSet._groups), 1)

		# but upon evaluation we see an update has been applied
		# note that this addition is ONE update - groups are not maintained
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[8, 10, 23], [34, 44]]
			)

		# Demonstrate slicing for columns works as expected
		self.assertEqual(
			[tuple(group) for group in c.results['c',:]],
			[(8, 10, 23), (34, 44)]
			)

		

def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(WindowTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)



if __name__ == '__main__':
    unittest.main()