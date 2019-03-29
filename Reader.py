import pandas as pd
from lxml import etree
import random
import xlrd as xl
import re
import sys
import math
import pprint

from pandas import ExcelWriter
from pandas import ExcelFile

from collections import defaultdict

idsByQNumber = ['9340801989528012','1254270073318589','7166764509411762','6982372828023634','3840918878658545','4557823883299363','6066305456973227','5238465176929965','1353821762883927','6316799452705066','4902017929748647','8724509422072051','7928992035871906','8579632148937093','1360611574931166','0275372271127625','9799376943258976','5971776243482026','8384403160475794','9733415024701857']

#Results data file - one per exam
dataFrameResults=pd.read_excel('AppPhysPharm_2015_results_for_analysis.xls', sheet_name='MSDLT Analysis', header=[1,2])
#Jon's file which links old 5x5 question name/ID with new 1 x 5 Perception question ID- one per subject
dataFrameSplittings=pd.read_excel('APP_Splitting_ReportNames.xlsx', sheet_name='Names', header=[0], converters={'QID':str,'New ID':str}) #convertors reads numberrs as strings
#Jon's file which gives correct answers and response_Ids as importe to Inspera
dataFrameMappings=pd.read_excel('APP_Summ_Migration_Report_Responses.xlsx', sheet_name='Report', header=[0], converters={'QID':str})
dataFrameResponseIds=pd.read_excel('APP_Summ_Migration_Report_Responses.xlsx', sheet_name='Response IDs', header=[0], converters={'QID':str,'OptionText':str})
# Jon's export of ALL perception questions for a topic so we have data for questions which haven't been exported to Inspera
dataNonImportedCorrectAnswers=pd.read_excel('APP_AllStatuses_report.xls', sheet_name='Sheet1', header=[0], converters={'QID':str})
# Mapping of SSOs to randomly generated IDs - will need to add exacjh new batch of students to this
studentIds=pd.read_excel('studentIds.xlsx', sheet_name='StudentIds', header=[0], converters={'QID':str})
# exported from Inspera of all importe questions PLUS the 125 dummy questions used to replace questions not imported to Inspera
questionSetTree = etree.parse("question_set_export/imsmanifest.xml") #XML dump of all questions PLUS our dummy question IN ONE FILE

#read in correct answers for all questions in bank - will be used to choose dummy question against which to store data where question has not beeen imported to Inspera

#begin by reading in dummy question data which we can use where questions in results set aren't in the imported questions
dData = defaultdict(list)
dummyQuestionTree = etree.parse("msdlt_dummy_questions/imsmanifest.xml")
find = etree.XPath("//*[contains(text(), $name)]")
elements = find(dummyQuestionTree, name="msdlt_dummy_")
for element in elements:
     #print('here')
     qName = str(element.text)
     qNameParts = qName.split('_')  # split up msdlt_dummy_a01
     if re.match('(a|b|c|d|e)\d{2}', qNameParts[2]):
          #print('name is' + qName)
          idElement = element.getparent().getparent().getparent().getparent().getparent()
          identifier = idElement.attrib['identifier']
          href = idElement.attrib['href']
          idParts = identifier.split('_')
          dData[qNameParts[2]] = [idParts[1],[]]  #key dict by e.g a01 where a = answer marked correct - add inspera question id at this point
          #now go and open up the appropriate xml file to read in the response_ids
          dummyResponseTree = etree.parse("msdlt_dummy_questions/"+href)
          for choice in dummyResponseTree.iter():
               if choice.tag=="{http://www.imsglobal.org/xsd/imsqti_v2p1}simpleChoice":
                    dData[qNameParts[2]][1].append(choice.attrib['identifier'])

#pprint.pprint(dData)

qData = defaultdict(list)

for index, row in dataFrameSplittings.iterrows():
     if row.loc['QID'] not in qData:
          # insert some data into dict referenced by original 5-part QID
          qData[row.loc['QID']] = [[],[],[],[]]  # 0 = new ID, 1 = new Title, 2 = Correct answer, 3 = Inspera QID
     # add values for our New ID and New Title
     qData[row.loc['QID']][0].append(row.loc['New ID'])
     qData[row.loc['QID']][1].append(row.loc['New Title'])
     # get row in dataFrameMappings for the New ID
     mappingRow = dataFrameMappings.loc[dataFrameMappings['QID']==row.loc['New ID']]
     if not mappingRow.empty:
          #print(mappingRow.iloc[0]['Answer'])
          qData[row.loc['QID']][2].append(mappingRow.iloc[0]['Answer'])
     else:
          qData[row.loc['QID']][2].append('nan')
     # now need to get inspera IDs
     #find = etree.XPath("//*[contains(text(), $name)]")
     elements = find(questionSetTree, name=row.loc['New Title'])  #find defined above does a 'contains' search
     if(len(elements)>0):
          element = elements[0].getparent().getparent().getparent().getparent().getparent().attrib['identifier']
          idParts = element.split('_')
          qData[row.loc['QID']][3].append(idParts[1])
     else:
          qData[row.loc['QID']][3].append('nan')

#pprint.pprint(qData)
#create series of data for each column
rData = pd.DataFrame(columns=['unique test id', 'unique candidate id', 'inspera Question ID', 'selectedResponse', 'unique response choice ID'])

uniqueTestId = random.getrandbits(64)

#print(dataFrameResults.columns)

###############################
# Stepping through student data
###############################

dummyQuestionCounters = {'a': 0, 'b': 0, 'c': 0, 'd': 0,'e': 0}  # NOTE All students need to be answering SAME dummy question replacement for each part of a question!
# so create dict which will track dummy question name used for this QID and stem
dummyQuestionByQID = defaultdict(list)  #new dict - keyed by ['QID'] and contains [stem1, stem2, stem3, etc]}
for index, row in dataFrameResults.iterrows():
     if isinstance(row.loc[('Description', 'Participant')],str) :
          #first check that we have an SSO in dataFrameResults.loc['Participant']
          if re.match('\w{4}\d{4}', row.loc[('Description', 'Participant')]):
               print(row.loc[('Description', 'Participant')])
               #let's collate data to add as row to dataframe
               #start with candidateId (testID set once above this)
               candidateRow = studentIds.loc[studentIds['SSO'] == row.loc[('Description', 'Participant')]]
               #print(candidateRow)
               if not candidateRow.empty:
                    uniqueCandidateId = candidateRow.iloc[0]['ID']
                    #We have a uniqueCandidateId, let's comlete rest of dataframe to append to rData
                    #Start by splitting answer for each question
                    questionCounter = 0
                    for count, (rindex, value) in enumerate(row.iteritems()):
                         if count > 9 and count % 2 == 0: #col 10 will contain first question in standard output then answers will be in even rows
                              QIDofQuestion = idsByQNumber[questionCounter]
                              #dealing with answers
                              #let's split the answers given
                              if isinstance(value,str):
                                   #print(value)
                                   answers = value.split(':')
                                   if (len(answers) == 5): #note assuming 5 parts MCQs - anything other than 5 means that we have a : in the answers given
                                        #loop through answers
                                        for aindex,answer in enumerate(answers):
                                             #Start by chceking whether stem has been answered
                                             #DYif len(answer) > 0:
                                             #DY#question has been answered
                                             # dealing with question
                                             # get insperaId
                                             notInQDataOrNotInrData = False
                                             if QIDofQuestion in qData: #chcek if this ID is a key in qData NOTE could still be missing from dataFrameResponseIds
                                                  #print(rindex[0])
                                                  #print(qData[QIDofQuestion][3][aindex])
                                                  insperaQuestionId = qData[QIDofQuestion][3][aindex]
                                                  #NEED to look up the response identifier from APP_Summ_Migration_Report_Responses.xlsx [Response IDs sheet]
                                                  #Let's get the actual question text by removing and A. at beginning
                                                  #print(answer)
                                                  if len(answer) > 0:
                                                       #ie student has answered this stem
                                                       if re.match('\w\.\s', answer):
                                                            answerText = re.sub(r'\w\.\s', r'', answer)
                                                       else:
                                                            answerText=answer
                                                       responseRows = dataFrameResponseIds.loc[dataFrameResponseIds['QID']==qData[QIDofQuestion][0][aindex]]
                                                       # check that we actually have a matching answer!!
                                                       if len(responseRows)>0:
                                                            responseRow = responseRows.loc[responseRows['OptionText']==answerText]
                                                            if len(responseRow) > 0:
                                                                 #print(responseRow)
                                                                 uniqueResponseChoiceId = responseRow.iloc[0]['OptionID']
                                                                 dataToAdd = pd.DataFrame({'unique test id': [uniqueTestId],
                                                                                     'unique candidate id': [uniqueCandidateId],
                                                                                     'inspera Question ID': [insperaQuestionId],
                                                                                     'selectedResponse': [answerText],
                                                                                     'unique response choice ID': [uniqueResponseChoiceId]})
                                                                 rData = rData.append(dataToAdd, ignore_index = True)
                                                                 #print(uniqueTestId, uniqueCandidateId, insperaQuestionId, answerText, uniqueResponseChoiceId)
                                                            else:
                                                                 #notInQDataOrNotInrData = True - need to sort this, can't chose dummy question unless applies to all students
                                                                 sys.exit('Problem with finding answer' + answerText + ' in ' + QIDofQuestion + ' new QID ' + qData[QIDofQuestion][0][aindex])
                                                       else:
                                                            #notInQDataOrNotInrData = True  - need to sort this, can't chose dummy question unless applies to all students
                                                            sys.exit('Problem with finding answer' + answerText + ' in ' + QIDofQuestion + ' new QID ' + qData[QIDofQuestion][0][aindex])
                                                  else:
                                                       #student hasn't answered this stem - enter as unanswered
                                                       uniqueResponseChoiceId = responseRow.iloc[0]['OptionID']
                                                       dataToAdd = pd.DataFrame({'unique test id': [uniqueTestId],
                                                                                 'unique candidate id': [uniqueCandidateId],
                                                                                 'inspera Question ID': [insperaQuestionId],
                                                                                 'selectedResponse': ['unanswered'],
                                                                                 'unique response choice ID': ['unanswered']})
                                                       rData = rData.append(dataToAdd, ignore_index=True)
                                             else:
                                                  notInQDataOrNotInrData = True
                                                  #print(QIDofQuestion + ' not in qData')

                                             if notInQDataOrNotInrData:
                                                  #Need to go and look it up in dataNonImportedCorrectAnswers
                                                  notImportedResponseRow = dataNonImportedCorrectAnswers.loc[dataNonImportedCorrectAnswers['QID'] == QIDofQuestion]
                                                  if len(notImportedResponseRow) > 0:
                                                       optionsByStem = notImportedResponseRow.iloc[0]['Options By Stem']
                                                       correctAnswersByStem = notImportedResponseRow.iloc[0]['Answers by Stem']
                                                       noOfOptionsForStems = optionsByStem.split(':')
                                                       correctAnswersForStems = correctAnswersByStem.split(':')
                                                       #Check whether noOfOptionsForStems are all 5
                                                       fivePerStem = True
                                                       aToEinStems = True
                                                       if len(noOfOptionsForStems) == 5:
                                                            fiveStems = True
                                                       else:
                                                            fiveStems = False
                                                       for checkIndex, noOfOptionsForStem in enumerate(noOfOptionsForStems):
                                                            if int(noOfOptionsForStem) != 5:
                                                                 fivePerStem = False
                                                            if not re.match('[ABCDE]', correctAnswersForStems[checkIndex]):
                                                                 aToEinStems = False
                                                       if fivePerStem:
                                                            if not aToEinStems:
                                                                 # Need to convert correctAnswersForStems so that they are A-E
                                                                 for correctIndex in range(len(correctAnswersForStems)):
                                                                      #step through each correctAnswer
                                                                      amountToReduceCorrectAnswerLetter = 0
                                                                      for innerCorrectIndex in range(correctIndex):
                                                                           amountToReduceCorrectAnswerLetter += int(noOfOptionsForStems[innerCorrectIndex])  # ie add no of options in previous stems
                                                                      correctAnswersForStems[correctIndex] = chr(ord(correctAnswersForStems[correctIndex].lower()) - amountToReduceCorrectAnswerLetter).upper()  # ie a=0, b=1

                                                            #create dummy question name
                                                            dummyQuestionAlreadyAssigned = False
                                                            if QIDofQuestion in dummyQuestionByQID and dummyQuestionByQID[QIDofQuestion][aindex] and dummyQuestionByQID[QIDofQuestion][aindex]!=None:
                                                                 #already have dummyQName for this question
                                                                 dummyQName = dummyQuestionByQID[QIDofQuestion][aindex]
                                                            else:
                                                                 # don't already have dummyQName for this question to work it out and add to dummyQuestionByQID
                                                                 lowerAnswerLetter = correctAnswersForStems[aindex].lower()
                                                                 if dummyQuestionCounters[lowerAnswerLetter] + 1 < 10:
                                                                      questionNumber = '0' + str(dummyQuestionCounters[lowerAnswerLetter] + 1)
                                                                 else:
                                                                      questionNumber = str(dummyQuestionCounters[lowerAnswerLetter] + 1)
                                                                 dummyQName = lowerAnswerLetter + questionNumber
                                                                 dummyQuestionCounters[lowerAnswerLetter] += 1 #increment counter for this letter
                                                                 if not dummyQuestionByQID[QIDofQuestion]:
                                                                      dummyQuestionByQID[QIDofQuestion] = [None]*20 #initiliase the list with far too many empty spaces
                                                                 dummyQuestionByQID[QIDofQuestion][aindex] = dummyQName #add to dummyQuestionByQID

                                                            if len(answer) > 0:
                                                                 # ie student has answered this stem
                                                                 #get answer letter from answerText
                                                                 if re.match('\w\.\s', answer):
                                                                      answerLetter = answer[0]
                                                                      answerText = re.sub(r'\w\.\s', r'', answer)
                                                                 else:
                                                                      sys.exit('Cant find an answer letter for ' + row.loc[('Description', 'Participant')] + ' in question ' + QIDofQuestion + ' for answer ' + answer)
                                                                 #convert letter to a number using fact that ord('a') = 97
                                                                 #NOTE That (for MCQs anyway, the letters will have been renumbered so that instead of A-Y across all five stems
                                                                 #they will now be A-E:A-E:A-E:A-E:A-E
                                                                 #...so...until we have another way of detecting where correct answer won't be A-E...
                                                                 #MCQ=True
                                                                 amountToReduceActualAnswerLetter = 0
                                                                 #if MCQ:
                                                                 #ie renumbered A-E
                                                                 for stemIndex in range(aindex):
                                                                      amountToReduceActualAnswerLetter += int(noOfOptionsForStems[stemIndex])  #ie add no of options in previous stems
                                                                 answerNumber = ord(answerLetter.lower())-97-amountToReduceActualAnswerLetter #ie a=0, b=1
                                                                 #NOTE THAT for questions where the same letters e.g A-E are repeated on two consecutive stems, answerNumber may be negative BUT
                                                                 # because it appears to then read backwards from the end of the array, this wil still choose the correct letter
                                                                 # AS LONG as both stems have the same number of options
                                                                 uniqueResponseChoiceId = dData[dummyQName][1][answerNumber]
                                                                 #if QIDofQuestion=='8384403160475794':
                                                                 #     print(QIDofQuestion, dummyQName, answerNumber, answerLetter, amountToReduceActualAnswerLetter, ord(answerLetter.lower()))
                                                                 dataToAdd = pd.DataFrame(
                                                                      {'unique test id': [uniqueTestId],
                                                                       'unique candidate id': [uniqueCandidateId],
                                                                       'inspera Question ID': [dData[dummyQName][0]],
                                                                       'selectedResponse': [answerText],
                                                                       'unique response choice ID': [
                                                                            uniqueResponseChoiceId]})
                                                                 rData = rData.append(dataToAdd, ignore_index=True)
                                                            else:
                                                                 # student hasn't answered this stem - enter as unanswered
                                                                 uniqueResponseChoiceId = responseRow.iloc[0][
                                                                      'OptionID']
                                                                 dataToAdd = pd.DataFrame(
                                                                      {'unique test id': [uniqueTestId],
                                                                       'unique candidate id': [uniqueCandidateId],
                                                                       'inspera Question ID': [dData[dummyQName][0]],
                                                                       'selectedResponse': ['unanswered'],
                                                                       'unique response choice ID': ['unanswered']})
                                                                 rData = rData.append(dataToAdd, ignore_index=True)

                                                       else:
                                                            #Not five by five question
                                                            sys.exit('Question ' + QIDofQuestion + ' not five by five question')
                                                  else:
                                                       sys.exit('Cant find ' + QIDofQuestion + ' in dataNonImportedCorrectAnswers')
                                                  #print(QIDofQuestion + ' not in qData or rData')
                                                  # question not in qData - will need to pick one of the resuable ones
                                                  # NEED TO MATCH Correct answer - so read from....qData[][2]
                                                  # plus track which ones used already
                                             #DYelse:
                                                  #no answer for this stem
                                                  # TODO need to make sure we deal with empty single stem answers which end up here BUT need to get insperaQuestionId OR dData[dummyQName][0]
                                             #DY     dataToAdd = pd.DataFrame(
                                             #DY          {'unique test id': [uniqueTestId],
                                             #DY           'unique candidate id': [uniqueCandidateId],
                                             #DY           'inspera Question ID': [dData[dummyQName][0]],
                                             #DY           'selectedResponse': ['unanswered'],
                                             #DY           'unique response choice ID': ['unanswered']})
                                             #DY     rData = rData.append(dataToAdd, ignore_index=True)
                                   else:
                                        sys.exit('Problem with splitting '+answers)
                              else:
                                   #NOTE That for situations whetre using a dummy question, are assuming that one has already been set up ie can't have whole missing question in first row of student results
                                   if math.isnan(value):
                                        if QIDofQuestion in qData:  # chcek if this ID is a key in qData NOTE could still be missing from dataFrameResponseIds
                                             for qID in qData[QIDofQuestion][3]:
                                                  insperaQuestionId = qID
                                                  dataToAdd = pd.DataFrame(
                                                       {'unique test id': [uniqueTestId],
                                                       'unique candidate id': [uniqueCandidateId],
                                                        'inspera Question ID': [insperaQuestionId],
                                                        'selectedResponse': ['unanswered'],
                                                        'unique response choice ID': ['unanswered']})
                                                  rData = rData.append(dataToAdd, ignore_index=True)
                                        else:
                                             #we're reusing dummyQuestion hopefully assigned in code for where we DO have an answer
                                             if QIDofQuestion in dummyQuestionByQID and len(dummyQuestionByQID[QIDofQuestion])>0:
                                                  for dummyQName in dummyQuestionByQID[QIDofQuestion]:
                                                       insperaQuestionId = dData[dummyQName][0]
                                                       dataToAdd = pd.DataFrame(
                                                            {'unique test id': [uniqueTestId],
                                                             'unique candidate id': [uniqueCandidateId],
                                                             'inspera Question ID': [insperaQuestionId],
                                                             'selectedResponse': ['unanswered'],
                                                             'unique response choice ID': ['unanswered']})
                                                       rData = rData.append(dataToAdd, ignore_index=True)
                                             else:
                                                  sys.exit('Problem with nor having any dummy insperaQuestionIds for ' + QIDofQuestion + '. Chcek that we have a fully answered first row of student results')
                                   else:
                                        print('Problem with answers for ' + row.loc[('Description', 'Participant')] + " with value " + str(value))
                              questionCounter = questionCounter + 1
               else:
                    sys.exit('Problem with ' + row.loc[('Description', 'Participant')] + ' need to add to studentIds')

#print(rData)
rData.to_excel("output.xlsx")
#print(dummyQuestionByQID)
#Now need to read in student answers and remark...
marksAchieved = defaultdict(list) #dict to store marks which we should see in answer sheet keyed by [SSO] then [totalmark,[markStem1, markStem2, etc]
for index, row in rData.iterrows():
     #first go and get SSO from unique candidate id
     candidateRow = studentIds.loc[studentIds['ID'] == row.loc['unique candidate id']]
     if not candidateRow.empty:
          candidateSSO = candidateRow.iloc[0]['SSO']
          if not candidateSSO in marksAchieved:
               #haven't yet added this candidate
               marksAchieved[candidateSSO]=[0,[]] #initialise total mark for question at 0
               print(candidateSSO)
          #Note that, to make this easier, require an XML dump of all questions PLUS our dummy question IN ONE FILE
          findByIndentifier = etree.XPath("//*[contains(@identifier, $name)]")
          elements = findByIndentifier(questionSetTree, name='ID_' + row.loc['inspera Question ID'])  # find defined above does a 'contains' search
          if (len(elements) > 0):
               href = elements[0].attrib['href']
               correctResponseTree = etree.parse("question_set_export/" + href)
               correctResponse = correctResponseTree.find('.//{http://www.imsglobal.org/xsd/imsqti_v2p1}correctResponse/{http://www.imsglobal.org/xsd/imsqti_v2p1}value')
               #print(correctResponse.text)
               if correctResponse.text == row.loc['unique response choice ID']:
                    #correct
                    marksAchieved[candidateSSO][0] += 1
                    marksAchieved[candidateSSO][1].append(1)
               else:
                    #incorect
                    marksAchieved[candidateSSO][1].append(0)
          else:
               sys.exit('Cant find inspera Question ID ' + row.loc['inspera Question ID'] + ' in imsmanifest.xml')
     else:
          sys.exit('Cant find ' + row.loc['unique candidate id'] + ' while checking')
# now go and check that marksAchieved match those in the original dataFrameResults
noOfMatches=0
for index, row in dataFrameResults.iterrows():
     if isinstance(row.loc[('Description', 'Participant')],str) :
          #first check that we have an SSO in dataFrameResults.loc['Participant']
          if re.match('\w{4}\d{4}', row.loc[('Description', 'Participant')]):
               if int(row.loc[('Description', 'Total score')]) != marksAchieved[row.loc[('Description', 'Participant')]][0]:
                    sys.exit('Total marks dont match for ' + row.loc[('Description', 'Participant')])
               else:
                    noOfMatches += 1
print('No of chceked student scores = ' + str(noOfMatches))






