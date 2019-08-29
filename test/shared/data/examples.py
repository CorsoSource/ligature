from shared.data.recordset import RecordSet



def genData(columns, rows, start=0):
    if not isinstance(columns, int):
        columns = len(columns)
    if columns == 1:
        return (i for i in range(start, start+rows*columns, columns))
    else:
        return (tuple(range(i,i+columns))
                for i 
                in range(start, start+rows*columns, columns))



a = [(1,2,3,4),(5,6),(7,8,9)]
b = [(0,1,0,1),(0,1),(0,1,0)]

simpleRecordSet = RecordSet(recordType='ab')
for g in zip(a,b):
    simpleRecordSet.append(v for v in zip(*g))

