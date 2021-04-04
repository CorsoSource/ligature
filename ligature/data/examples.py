from .recordset import RecordSet


def genData(columns, rows, start=0):
    if not isinstance(columns, int):
        columns = len(columns)
    if columns == 1:
        return (i for i in range(start, start+rows*columns, columns))
    else:
        return (tuple(range(i,i+columns))
                for i 
                in range(start, start+rows*columns, columns))



a1 = [(1,2,3,4),(5,6),(7,8,9)]
b1 = [(0,1,0,1),(0,1),(0,1,0)]

a2 = [(11,12,13),(14,15,16)]
b2 = [(1,0,1),(0,1,0)]

simpleRecordSet = RecordSet(recordType='ab')
for g in zip(a1,b1):
    simpleRecordSet.append(v for v in zip(*g))

simpleAddition = RecordSet(recordType='ab')
for g in zip(a2,b2):
    simpleAddition.append(v for v in zip(*g)) 