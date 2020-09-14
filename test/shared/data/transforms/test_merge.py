import unittest


from shared.data.recordset import RecordSet

from shared.data.transforms.merge import Merge


class MergeTestCase(unittest.TestCase):

    def test_basic(self):

        rsa = [(1,2,3,4),(5,6),(7,8,9)]
        rsb = [(0,1,0,1),(0,1),(0,1,0)]
        rsc = [(9,8,7,6,5),(4,3,2),(1,)]
        rsd = [(1,0,1,0,1),(0,1,0),(1,)]

        rs1 = RecordSet(recordType='ab')
        for g in zip(rsa,rsb):
            rs1.append(v for v in zip(*g))
        
        rs2 = RecordSet(recordType='cb')
        for g in zip(rsc,rsd):
            rs2.append(v for v in zip(*g))

        merge = Merge([rs1, rs2])

        self.assertEqual(
            merge.results._RecordType._fields,
            ('a', 'b', 'c')
            )

        self.assertEqual(
            merge.results._groups[0][0]._tuple,
            (1, 1, 9) 
            )

        self.assertEqual(
            [[record._tuple for record in group] for group in merge],
            [[(1, 1, 9),
              (2, 0, 8),
              (3, 1, 7),
              (4, 0, 6),
              (5, 1, 5),
              (6, 0, 4),
              (7, 1, 3),
              (8, 0, 2),
              (9, 1, 1)]]
            )



def runTests():
    suite = unittest.TestLoader().loadTestsFromTestCase(MergeTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
