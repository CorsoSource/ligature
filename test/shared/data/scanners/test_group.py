import unittest


from shared.data.recordset import RecordSet
from shared.data.examples import simpleRecordSet, simpleAddition

from shared.data.scanners.group import GroupScanner


class GroupScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = GroupScanner(srs,'a')

		# Scanners are like generators...
		self.assertEqual(
			[[record._tuple for record in group] for group in scanner], 
			[[(1, 0), (2, 1), (3, 0), (4, 1)], [(5, 0), (6, 1)], [(7, 0), (8, 1), (9, 0)]]
			)

		# ... and will exhaust when fully consumed
		self.assertEqual(
			[group for group in scanner],
			[]
			)

		srs.extend(simpleAddition)

		# adding data means the scanner consumes the new data
		self.assertEqual(
			[[record._tuple for record in group] for group in scanner],
			[[(11, 1), (12, 0), (13, 1)], [(14, 0), (15, 1), (16, 0)]]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[len(group) for group in scanner], 
			[4, 2, 3, 3, 3]
			)



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(GroupScannerTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)
