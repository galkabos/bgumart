from persistence import *

import sys

def main(args : list):
    inputfilename : str = args[1]
    with open(inputfilename) as inputfile:
        for line in inputfile:
            splittedline : list = line.strip().split(", ")
            #splittedline[0] = productId
            #splittedline[1] = units, if positive it means supplies and if negative is means sell 
            #splittedline[2] = if(splittedline[1]>0) supplierId, if(splittedline[1]<0) employeeId
            #splittedline[3] = date
            x = repo.products.getQuantity(splittedline[0])
            if(int(splittedline[1])>0):
                repo.activities.insert(Activitie(splittedline[0],splittedline[1],splittedline[2],splittedline[3]))
                repo.products.update(x +int(splittedline[1]),int(splittedline[0]))
            else:
                if(x + int(splittedline[1])>0):
                    repo.activities.insert(Activitie(splittedline[0],splittedline[1],splittedline[2],splittedline[3]))
                    repo.products.update(x + int(splittedline[1]),int(splittedline[0]))

if __name__ == '__main__':
    main(sys.argv)