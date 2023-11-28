import pandas as pd
df = pd.read_excel("104_excelv1.xls")

df = df.drop(0)
data_dict = {}
#遍历df每一行

for row in df.iterrows():
    info_address = row[1][0]
    #转为10进制
    info_address_10 = str(int(info_address,16))
    info_name = row[1][1]
    data_range = row[1][2]   
    data_dict[info_address_10] = [info_name,data_range]
for key, value in data_dict.items():
    print(key, value)

