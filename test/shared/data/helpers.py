def genData(columns, rows, start=0):
    if not isinstance(columns, int):
        columns = len(columns)
    if columns == 1:
        return (i for i in range(start, start+rows*columns, columns))
    else:
        return (tuple(range(i,i+columns))
                for i 
                in range(start, start+rows*columns, columns))