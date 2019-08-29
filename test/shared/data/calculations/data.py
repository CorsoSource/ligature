"""
	Test data
"""

from shared.data.recordset import RecordSet

a = [(1,2,3,4),(5,6),(7,8,9)]
b = [(0,1,0,1),(0,1),(0,1,0)]

simpleRecordSet = RecordSet(recordType='ab')
for g in zip(a,b):
    simpleRecordSet.append(v for v in zip(*g))

