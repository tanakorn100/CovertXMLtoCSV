import xml.etree.ElementTree as ET
import csv
import os.path

from time import gmtime, strftime
from shutil import copyfile



dateF = strftime("%Y%m%d", gmtime())
dtTimeStamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())


def GETTextFromTagXML(tagData):
    head = []
    row = []

    
    for element in range(len(tagData)):
        
        if list(tagData[element]):  # check child element in XML resources
            elementTagData = tagData[element]
            for data in range(len(elementTagData)):
                
                if list(elementTagData[data]):  # check subchild element in XML resources
                    valuedata = elementTagData[data]
                    for subValueData in range(len(valuedata)):
                        
                        if list(valuedata[subValueData]):  # check subchild element in XML resources
                            valuedata_2layers = valuedata[subValueData]
                            
                            for subValuedata_2layers in range(len(valuedata_2layers)):
                                head.append(tagData.tag[:14] + "_" + elementTagData.tag[:14] + "_" + valuedata.tag[:14] + "_" + valuedata_2layers.tag[:14] + "_" + valuedata_2layers[subValuedata_2layers].tag[:14])  
                                row.append(valuedata_2layers[subValuedata_2layers].text) #.find('x').find('xx').find('xxx').find('xxx').text)
                        else:
                            head.append(tagData.tag[:14] + "_" + elementTagData.tag[:14] + "_" + valuedata.tag[:14] + "_" + valuedata[subValueData].tag[:14])  
                            row.append(valuedata[subValueData].text) #.find('x').find('xx').find('xxx').text)
                
                else:
                    head.append(tagData.tag[:14] + "_" + elementTagData.tag[:14] + "_" + elementTagData[data].tag[:14]) 
                    row.append(elementTagData[data].text)   #.find('x').find('xx').text)
        
        else:
            head.append(tagData.tag[:14] + "_" + tagData[element].tag[:14])  
            row.append(tagData[element].text)  #.find('x')..text)
    
    
    return head, row



def writeHeaderAndRecordToFile(header , record , head , row):
    for sub in range(len(header)):
        head.append(header[sub])
        row.append(record[sub])

        
def GetTextAndTagFromUsersTag(users):    
    for users in users.findall('users'):
        user = users[0]
        for elementUser in range(len(user)):
            if list(user[elementUser]) :
                elementValueUser = user[elementUser]
                header , record = GETTextFromTagXML(elementValueUser)
                writeHeaderAndRecordToFile(header , record)
                
            else:
                head.append(user[elementUser].tag[:14])
                row.append(user[elementUser].text) #.find('username').text)
                
                
def GETSchedulerAPIwithManyQueue(pathGETInfo) :   
    pathGETInfo = pathGETInfo
    tree = ET.parse(pathGETInfo)
    root = tree.getroot()

    report = open("/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/SchedulerAPI_queue_"+str(dateF)+"_"+str(pathGETInfo[-5])+".csv", 'w')
    csvwriter = csv.writer(report)
    
    for queue in root.findall('queue'):
        head = []
        row = []

        for elementQueue in range(len(queue)): 
            if list(queue[elementQueue]) :
                elementValueQueue = queue[elementQueue]
                if elementValueQueue.tag == 'users' :
                    GetTextAndTagFromUsersTag(elementValueQueue)
                #### check tag name and send to them tag
                else:
                    header , record = GETTextFromTagXML(elementValueQueue)
                    writeHeaderAndRecordToFile(header , record , head , row )     
            else:
                head.append(queue[elementQueue].tag)
                row.append(queue[elementQueue].text)

    if 'users' in head:
        index_row = head.index("users")
        head.remove('users')
        del row[index_row]
    head.insert(0, 'run_timestamp')
    #row.insert(0, dtTimeStamp)

    csvwriter.writerow(head)
    csvwriter.writerow(row)       

    report.close()

    ###############################################################################################################
    
def writeRowIntoSchedulerReport(pathInsrt):
    
    pathInsrt = pathInsrt
    pathMaster = '/home/kaoper/tmp/fileMaster/fileMaster.csv'
    
    dataMaster = open(pathMaster).readline().rstrip('\n')
    dataInsrt = open(pathInsrt).readlines()
    valDataInsrt = list(dataInsrt)[1].rstrip('\n')
    headDataInsrt = list(dataInsrt)[0].rstrip('\n')

    lstValDataInsrt = valDataInsrt.split(',')
    lstDataMaster = dataMaster.split(',')
    lstHeadDataInsrt = headDataInsrt.split(',')


    rowAppToReport = []
    indexOfDataMaster = []
    indexValDataInsrt = 0


    if len(lstDataMaster) > len(lstHeadDataInsrt) :

        for elementHDI in lstHeadDataInsrt:
            if elementHDI in lstDataMaster:
                indexOfDataMaster.append(lstDataMaster.index(elementHDI)) # get index in element hawq 
            else : 
                continue


        for index in range(len(lstDataMaster)):
            if index in indexOfDataMaster:
                rowAppToReport.append(lstValDataInsrt[indexValDataInsrt])
                indexValDataInsrt += 1
            else : 
                rowAppToReport.append("-")
    else:
        for num in range(len(lstValDataInsrt)):
            rowAppToReport.append(lstValDataInsrt[num])

    
    rowAppToReport.insert(0, dtTimeStamp)

    report = open("/home/kaoper/reportOutput/logYarnResource_"+str(dateF)+"_SchedulerAPI.csv", 'a+')
    csvwriter = csv.writer(report)
    csvwriter.writerow(rowAppToReport) 

    report.close()
    

    
    ###############################################################################################################
    
    
def GETSchedulerAPIwithROOTqueue(pathRootXML):
    
    tree = ET.parse(pathRootXML)
    root = tree.getroot()

    #f = open('/tmp/testScheduler/queueroot.csv', 'w')

    #csvwriter = csv.writer(f)

    for schedulerInfo in root.findall('schedulerInfo'):
        row = []
        head = []

    
        # GET Header of root queue by config get information 
        head.append(schedulerInfo.find('capacity').tag)
        head.append(schedulerInfo.find('usedCapacity').tag)
        head.append(schedulerInfo.find('maxCapacity').tag)
        head.append(schedulerInfo.find('queueName').tag)

        # GET text (value) of root queue 
        row.append(schedulerInfo.find('capacity').text)
        row.append(schedulerInfo.find('usedCapacity').text)
        row.append(schedulerInfo.find('maxCapacity').text)
        row.append(schedulerInfo.find('queueName').text)


        # GET Header and text of root queue and " capacities "   tag by automatic get information 
        for capacities in schedulerInfo.findall('capacities'):
            queueCapacitiesByPartition = capacities[0]

            for tag in range(len(queueCapacitiesByPartition)):
                head.append(queueCapacitiesByPartition.tag[:15] + "_" +queueCapacitiesByPartition[tag].tag)
                row.append(queueCapacitiesByPartition[tag].text)

        # GET Header and text of root queue and " health "  tag by automatic get information 
        for health in schedulerInfo.findall('health'):
            operationsInfo = health[1]

            #print(health.find('lastrun').text)
            row.append(health.find('lastrun').text)

            for posEntry in range(len(operationsInfo)):   # get information " operationsINFO " 
                entry = operationsInfo[1]
                valueOfEntry = entry[1]
                for posValueInEntry in range(len(valueOfEntry)):
                    row.append(operationsInfo[posEntry][1][posValueInEntry].text)

            posLstRunD = len(health)-3

            for eachLstRunD in range(posLstRunD,len(health)):  # get information " lastRunDetails "  by config 3 group of header 
                lastRunDetails = health[eachLstRunD]

                row.append(lastRunDetails.find('count').text)
                row.append(lastRunDetails.find('resources').find('memory').text)
                row.append(lastRunDetails.find('resources').find('vCores').text)
        
        headerHealth = ['health_lastrun','operInfo_last-preemption_nodeId','operInfo_last-preemption_containerId',\
                       'operInfo_last-preemption_queue','operInfo_last-reservation_nodeId','operInfo_last-reservation_containerId',\
                       'operInfo_last-reservation_queue','operInfo_last-allocation_nodeId','operInfo_last-allocation_containerId',\
                       'operInfo_last-allocation_queue','operInfo_last-release_nodeId','operInfo_last-release_containerId',\
                       'operInfo_last-release_queue','lastRunDetails_releases_count','lastRunDetails_releases_memory','lastRunDetails_releases_vCores',\
                       'lastRunDetails_allocations_count','lastRunDetails_allocations_memory','lastRunDetails_allocations_vCores',\
                       'lastRunDetails_reservations_count','lastRunDetails_reservations_memory','lastRunDetails_reservations_vCores']
        for header in headerHealth:
            head.append(header)
            
            
        #csvwriter.writerow(head)          
        #csvwriter.writerow(row)  

    #f.close()

    print(" *********** Writing SchedulerAPI Report File [Root part] ***********")
    print("Writing file : "+pathRootXML)
    
    head.insert(0, 'run_timestamp')
    row.insert(0, dtTimeStamp)

    report = open("/home/kaoper/reportOutput/logYarnResource_"+str(dateF)+"_SchedulerAPI_[root].csv", 'a+')
    csvwriter = csv.writer(report)
    #csvwriter.writerow(head)
    csvwriter.writerow(row) 

    report.close()
    
    print(" *********** Complete Write SchedulerAPI Report File [Root part] ***********")
    
    
    

                
  ################################################   MAIN OF PROGRAM  #####################################################################


# count XML file in diredtory path before convert 
path = "/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/xml"
num_filesXML = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])

listNameFileQueueXML = 1
for fileXML in range(num_filesXML):
    pathGETInfo = "/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/xml/SchedulerAPI_queue_"+str(dateF)+"_"+str(listNameFileQueueXML)+".xml"
    listNameFileQueueXML += 1

    # get file name to convert XML to CSV 
    GETSchedulerAPIwithManyQueue(pathGETInfo)

# count CSV file in directory path after convert that store in here.

path = "/home/kaoper/tmp/SchedulerAPI/"+str(dateF)
num_filesCSV = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])

# GET file name in directory after convert  to  write data into report. 

listNameFileQueueCSV = 1
for fileCSV in range(num_filesCSV):
    pathInsrt = "/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/SchedulerAPI_queue_"+str(dateF)+"_"+str(listNameFileQueueCSV)+".csv"
    listNameFileQueueCSV += 1

    writeRowIntoSchedulerReport(pathInsrt)


# GET root queue .xml into function for convert XML->CSV and write data into report.

pathRootXML =  "/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/xml/root/SchedulerAPI_queue_"+str(dateF)+"_root.xml"

GETSchedulerAPIwithROOTqueue(pathRootXML)
