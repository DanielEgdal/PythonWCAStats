# wrs.csv was created with the following SQL Query:
#select r.eventId, c.end_date, c.year, r.best, r.average, r.personid, regionalSingleRecord, regionalAverageRecord
#from results r
#join competitions c 
#on c.id = r.competitionid
#where ((r.regionalSingleRecord = 'WR') or (r.regionalAverageRecord = 'WR')) and r.eventid not in ('mmagic','magic','333ft','333mbo')

results = open('WCA_export_RanksAverage.tsv').readlines()[1:]
for i in range(len(results)):
    results[i] = results[i].split('\t')
    results[i][-1] = results[i][-1].strip('\n')

sresults = open('WCA_export_RanksSingle.tsv').readlines()[1:]
for i in range(len(sresults)):
    sresults[i] = sresults[i].split('\t')
    sresults[i][-1] = sresults[i][-1].strip('\n')

persons = open('WCA_export_Persons.tsv').readlines()[1:]
for i in range(len(persons)):
    persons[i] = persons[i].split('\t')
    persons[i][-1] = persons[i][-1].strip('\n')

wrs = open('wrs.csv').readlines()[1:]
for i in range(len(wrs)):
    wrs[i] = wrs[i].split(',')
    wrs[i][-1] = wrs[i][-1].strip('\n')

wrs.sort(key=lambda wrs:wrs[1])

from collections import defaultdict
dic = defaultdict()

# Below sets up for nested dictionaries, first for the person, then event, then single of average.
for things in sresults:
    dic[things[0]] = defaultdict()

for things in sresults:
    dic[things[0]][things[1]] = defaultdict()
    
for things in sresults:
    dic[things[0]][things[1]]['s'] = int(things[2])

for things in results:
    dic[things[0]][things[1]]['a'] = int(things[2])

score = defaultdict() # Update this to the last WR they have beaten.
fail = defaultdict() # The first WR they fail to be better than.

for id in persons:
    i = 0
    help = True
    try: # Some people have not done the event or is missing an average.
        while help: # While loop to help time complexity (once they fail their first WR it goes on to the next person).
            if wrs[i][6] == 'WR': 
                if dic[id[0]][wrs[i][0]]['s'] <= int(wrs[i][3]):
                    score[id[0]] = wrs[i][1]
                else:
                    fail[id[0]] = [wrs[i][1],wrs[i][0],wrs[i][3],'s']
                    help = False
            if wrs[i][7] == 'WR':
                if dic[id[0]][wrs[i][0]]['a'] <= int(wrs[i][4]):
                    score[id[0]] = wrs[i][1]
                else:
                    fail[id[0]] = [wrs[i][1],wrs[i][0],wrs[i][4],'a']
                    help = False
            i += 1
    except:
        fail[id[0]] = [wrs[i][1],wrs[i][0],'Either S/A']
        help = False
        i+=1

# Below is exporting the results, this could be done more effeciently but I opened a new python file to not have to re-run everything each time.
import csv
with open('yearnemesize','w') as file:
    writer = csv.writer(file)
    for keys in score:
        try:
            writer.writerow((keys,score[keys],fail[keys]))
        except:
            pass

results = open('yearnemesize').readlines()

for i in range(len(results)):
    results[i] = results[i].split(',')
    results[i][-1] = results[i][-1].strip('\n')

results.sort(key=lambda results:results[1], reverse= True)

with open('nemeresults.txt','w') as file:
    for i in results:
        if i[1] > '2003-08-24':
            file.write(str(i))
            file.write('\n')
