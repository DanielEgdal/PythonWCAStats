# Get the data ("competition_overview.csv" from the db with this query and save it to a csv
# select r.personid, c.countryid, c.id, c.start_date
# from results r
# join competitions c
# on r.competitionid = c.id
# group by r.personid, c.countryid, c.id, c.start_date
# order by r.personid, c.start_date


import pandas as pd 
from collections import defaultdict
import numpy as np

d = pd.read_csv('competition_overview.csv',parse_dates=['start_date'])

lis = defaultdict(list) # The current streak of countries
ov = defaultdict(int) # The counter for the longest streak

for val in d.values:
    if val[1][0] != 'X': # Remove multi-contry comps
        if val[1] not in lis[val[0]]: # If this is a new country in the current streak
            lis[val[0]].append(val[1])
        else:
            for idx, co in enumerate(lis[val[0]]):
                if val[1] == co:
                    cut = idx # The index from where the current streak has to reset
                    break
            lis[val[0]] = lis[val[0]][cut+1:] # Updating the list to start a new streak with non overlapping countries
            lis[val[0]].append(val[1])

        if ov[val[0]] < len(lis[val[0]]): # Updating the score
            ov[val[0]] = len(lis[val[0]])

# Sorting stuff to get an overview in the end
sov = list(ov.items())

ssov = sorted(sov, key=lambda x: x[1], reverse=True)

for i in ssov[:25]:
    print(i)
