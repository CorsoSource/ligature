import unittest


from ligature.recordset import RecordSet
from ligature.examples import simpleRecordSet, simpleAddition

from ligature.scanners.element import ElementScanner


class ElementScannerTestCase(unittest.TestCase):

	def test_basic(self):

		srs = RecordSet(simpleRecordSet)
		
		scanner = ElementScanner(srs,'a')

		# Scanners are like generators...
		self.assertEqual(
			[v for v in scanner], 
			[1, 2, 3, 4, 5, 6, 7, 8, 9]
			)

		# ... and will exhaust when fully consumed
		self.assertEqual(
			[v for v in scanner],
			[]
			)

		srs.extend(simpleAddition)

		# adding data means the scanner consumes the new data
		self.assertEqual(
			[v for v in scanner],
			[11, 12, 13, 14, 15, 16]
			)

		# resetting the scanner means it will replay the whole dataset
		scanner.reset()

		self.assertEqual(
			[v for v in scanner],
			[1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16]
			)



def runTests():
	suite = unittest.TestLoader().loadTestsFromTestCase(ElementScannerTestCase)
	unittest.TextTestRunner(verbosity=2).run(suite)



if __name__ == '__main__':
    unittest.main()