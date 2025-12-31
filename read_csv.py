import csv
dir = "C:/Users/PC/Downloads/"
file = "customers-100.csv"
with open(dir+file,'r',encoding='gb2312') as file:
    reader = csv.reader(file, delimiter=',', quotechar='|',skipinitialspace=True)
    data_list = list(reader)
data_list.pop(0)
for data in data_list:
    if int(data[0])<=10:
        print(data)