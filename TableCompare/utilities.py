from operator import itemgetter
import os
import json

def compare(sourceResultList, targetResultList, matchFile, unMatchFile,key):
    MatchFile   = open(matchFile, "a",encoding='utf-8')
    UnMatchFile = open(unMatchFile, "a",encoding='utf-8')
    sortedSourceResultList = sorted(sourceResultList, key=itemgetter(key))
    sortedTargetResultList = sorted(targetResultList, key=itemgetter(key))
    targetPointer = 0;
    sourcePointer = 0;
    targetLength = len(sortedTargetResultList);
    sourceLength = len(sortedSourceResultList);
    if (sourceLength > 0):
        sourceItem = sortedSourceResultList[sourcePointer]
    if (targetLength > 0):
        targetItem = sortedTargetResultList[targetPointer]
    while (targetPointer < targetLength and sourcePointer < sourceLength):
        sourceItem = sortedSourceResultList[sourcePointer]
        targetItem = sortedTargetResultList[targetPointer]
        if (targetItem == None):
            continue
        else:
            if (str.strip(sourceItem[key]) == str.strip(targetItem[key])):
                targetPointer  = targetPointer + 1
                sourcePointer  = sourcePointer + 1
                if (targetItem == sourceItem):
                    MatchFile.write(sourceItem[key] + '\n')
                else:
                     for item in sourceItem:
                         if (sourceItem[item] != targetItem[item]):
                             UnMatchFile.write(key + ' ' + item + ' ' + json.dumps(sourceItem[item]) + ' ' + json.dumps(targetItem[item]) + '\n')
            elif (str.strip(sourceItem[key]) < str.strip(targetItem[key])):
                sourcePointer  = sourcePointer + 1
                UnMatchFile.write(sourceItem[key] + ' Not Found in Target' + '\n')                                                   
            else:
                targetPointer  = targetPointer + 1
                UnMatchFile.write(targetItem[key] + ' Not Found in Source' + '\n')
    while (targetPointer < targetLength):
        targetPointer  = targetPointer + 1
        UnMatchFile.write(targetItem[key] + ' Not Found in Source' + '\n')
    while (sourcePointer < sourceLength):
        sourcePointer  = sourcePointer + 1
        UnMatchFile.write(sourceItem[key] + ' Not Found in Target' + '\n')
    MatchFile.close()
    UnMatchFile.close()
