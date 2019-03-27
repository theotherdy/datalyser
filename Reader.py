import pandas as pd
from lxml import etree
import random
import xlrd as xl
import re
import sys

from pandas import ExcelWriter
from pandas import ExcelFile

from collections import defaultdict

dataFrameResults=pd.read_excel('1BM2_APP2014_ResultsForAnalysis.xls', sheet_name='MSDLT Analysis', header=[1,2])
dataFrameSplittings=pd.read_excel('APP_Splitting_ReportNames.xlsx', sheet_name='Names', header=[0], converters={'New ID':str})
dataFrameMappings=pd.read_excel('APP_Summ_Migration_Report_Responses.xlsx', sheet_name='Report', header=[0], converters={'QID':str})
studentIds=pd.read_excel('studentIds.xlsx', sheet_name='StudentIds', header=[0], converters={'QID':str})
tree = etree.parse("APP_imsmanifest.xml")

qData = defaultdict(list)

for index, row in dataFrameSplittings.iterrows():
     if row.loc['Old Title'] not in qData:
          # insert some data
          qData[row.loc['Old Title']] = [[],[],[],[]]  # 0 = new ID, 1 = new Title, , 2 = Correct answer, 3 = Inspera QID
     # add values for our New ID and New Title
     qData[row.loc['Old Title']][0].append(row.loc['New ID'])
     qData[row.loc['Old Title']][1].append(row.loc['New Title'])
     # get row in dataFrameMappings for the New ID
     mappingRow = dataFrameMappings.loc[dataFrameMappings['QID']==row.loc['New ID']]
     if not mappingRow.empty:
          #print(mappingRow.iloc[0]['Answer'])
          qData[row.loc['Old Title']][2].append(mappingRow.iloc[0]['Answer'])
     else:
          qData[row.loc['Old Title']][2].append('nan')
     # now need to get inspera IDs
     find = etree.XPath("//*[contains(text(), $name)]")
     elements = find(tree, name=row.loc['New Title'])
     if(len(elements)>0):
          element = elements[0].getparent().getparent().getparent().getparent().getparent().attrib['identifier']
          idParts = element.split('_')
          qData[row.loc['Old Title']][3].append(idParts[1])
     else:
          qData[row.loc['Old Title']][3].append('nan')

print(qData)
#create series of data for each column
rData = pd.DataFrame(columns=['unique test id', 'unique candidate id', 'inspera Question ID', 'selectedResponse', 'unique response choice ID'])

uniqueTestId = random.getrandbits(64)

#print(dataFrameResults.columns)

thereWasAProblem = False

for index, row in dataFrameResults.iterrows():
     if isinstance(row.loc[('Description', 'Participant')],str) :
          #first check that we have an SSO in dataFrameResults.loc['Participant']
          if re.match('\w{4}\d{4}', row.loc[('Description', 'Participant')]):
               #let's collate data to add as row to dataframe
               #start with candidateId (testID set once above this)
               candidateRow = studentIds.loc[studentIds['SSO'] == row.loc[('Description', 'Participant')]]
               #print(candidateRow)
               if not candidateRow.empty:
                    uniqueCandidateId = candidateRow.iloc[0]['ID']
                    #We have a uniqueCandidateId, let's comlete rest of dataframe to append to rData
                    #Start by splitting answer for each question
                    for count, (rindex, value) in enumerate(row.iteritems()):
                         if count > 9 and count % 2 == 0: #col 10 will contain first question in standard output then answers will be in even rows
                              #dealing with answers
                              #let's split the answers given
                              answers = value.split(':')
                              if (len(answers) == 5): #note assuming 5 parts MCQs - anything other than 5 means that we have a : in the answers given
                                   #loop through answers
                                   for aindex,answer in enumerate(answers):
                                        # dealing with question
                                        # get insperaId
                                        if index==1:
                                             print(rindex[0])
                                             print(qData[rindex[0]]) #rindex is tuple - [0] contains old question name
                                             print(aindex,answer)
                              else:
                                   sys.exit('Problem with splitting '+answers)


                              #print(count, rindex, value, )
               else:
                    sys.exit('Problem with ' + row.loc[('Description', 'Participant')] + ' need to add to studentIds')

print(rData)



