### header ###
import re
import xml.etree.cElementTree as ET
from time import gmtime, strftime

dateF = strftime("%Y%m%d", gmtime())

tree = ET.parse("/home/kaoper/reportOutput/logYarnResources/xml/log_yarn_resource_"+str(dateF)+"_scheduler.xml")
root = tree.getroot()
xmlstr = ET.tostring(root, encoding='utf8', method='xml')
xmlstr = xmlstr.decode("utf-8") 

### variable ###
xmlnew = ''
xmlqueue = ''
xmlrootstr = ''
start = 0
countq = 0
openTagIndex = []

### convert xml to list ### 
for i in range(len(xmlstr)-1):
    if xmlstr[i] == ">" and xmlstr[i+1] == "<":
            xmlnew += xmlstr[i]
            xmlnew += "\n"
    else:
        xmlnew += xmlstr[i]
xmlnew = xmlnew.split("\n")
xmlroot = xmlnew


### find OpenTag and CloseTag ### 
for j in range(len(xmlnew)-1):
    if re.findall("capacitySchedulerLeafQueueInfo",xmlnew[j]):
        openTagIndex.append(str(j))
    elif re.match("</queue>",xmlnew[j]) and re.findall("preemptionDisabled",xmlnew[j-1]):
        countq += 1
        index = (len(openTagIndex)-1)
        start = int(openTagIndex[index])
        for k in range(len(xmlnew)): ### insert begin OpenTag to CloseTag
            if k >= start and k <= j:
                xmlqueue += xmlnew[k]
                xmlroot.pop(k) ### Pop index of list in XML(copy) ###
                xmlroot.insert(k,"")
        f = open("/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/xml/SchedulerAPI_queue_"+str(dateF)+"_"+str(countq)+".xml", 'w') ### Write XML queue by queue (Set output)###
        f.write('<queues xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" >')
        f.write(str(xmlqueue))
        f.write("</queues>")
        f.close()
        xmlqueue = ""  ### Reset variable ### 

        
### convert list to string and Write XML(root) ###
for l in range(len(xmlroot)-1):
    xmlrootstr += xmlroot[l]
g = open("/home/kaoper/tmp/SchedulerAPI/"+str(dateF)+"/xml/root/SchedulerAPI_queue_"+str(dateF)+"_root.xml", 'w') ### Output Path ###
g.write(xmlrootstr)
g.write("</scheduler>")
g.close()


