import subprocess
import datetime

print("############ NC DOWNLOADER UTILITY ################")
bucket = input('Select Satellite- \n 1. GOES-16 \n 2. GOES-17 \n : ')

bucketName1 = "gcp-public-data-goes-16"
bucketName2 = "gcp-public-data-goes-17"

bucketName = bucketName1 if bucket == '1' else bucketName2

startDateStr = input("Enter Start Date (MM/DD/YY): ")
startDateObj = datetime.datetime.strptime(startDateStr, '%m/%d/%y')
startDayCount = (startDateObj - datetime.datetime(startDateObj.year, 1, 1)).days + 1

endDateStr = input("Enter End Date (MM/DD/YY): ")
endDateObj = datetime.datetime.strptime(endDateStr, '%m/%d/%y')
endDayCount = (endDateObj - datetime.datetime(endDateObj.year, 1, 1)).days + 1

totalDays = endDayCount - startDayCount
glmDataType = "GLM-L2-LCFA"
year = str(startDateObj.year)

while totalDays >= 0:
    day = str(startDayCount).zfill(3)
    path = "gs://" + bucketName + "/" + glmDataType + "/" + year + "/" + day
    completed = subprocess.run('gsutil -m cp -r ' + path + ' .', shell=True)
    #print(path)
    totalDays -= 1
    startDayCount += 1
    if completed.returncode != 0:
        break
