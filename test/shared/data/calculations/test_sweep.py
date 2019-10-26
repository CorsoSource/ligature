import unittest


from shared.data.recordset import RecordSet
from shared.data.examples import simpleRecordSet, simpleAddition

from shared.data.calculations.sweep import Sweep


class SweepTestCase(unittest.TestCase):

	def test_basic(self):

		function = lambda a,b: a+b

		srs = RecordSet(simpleRecordSet)

		c = Sweep([srs], function, 'c')

		# Calculations are lazily evaluated
		self.assertEqual(c._resultSet._groups, [])

		# When evaluated, we get the following
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[1, 3, 3, 5, 5, 7, 7, 9, 9]]
			)

		srs.extend(simpleAddition)

		# adding data from a source doesn't immediately update
		self.assertEqual(len(c._resultSet._groups), 1)

		# but upon evaluation we see an update has been applied
		# note that this is ONE update - groups are not maintained
		self.assertEqual(
			[[v.c for v in group] for group in c.results.groups],
			[[1, 3, 3, 5, 5, 7, 7, 9, 9], [12, 12, 14, 14, 16, 16]]
			)

		# Demonstrate slicing for columns works as expected
		self.assertEqual(
			[tuple(group) for group in c.results['c',:]],
			[(1, 3, 3, 5, 5, 7, 7, 9, 9), (12, 12, 14, 14, 16, 16)]
			)


suite = unittest.TestLoader().loadTestsFromTestCase(SweepTestCase)
unittest.TextTestRunner(verbosity=2).run(suite)