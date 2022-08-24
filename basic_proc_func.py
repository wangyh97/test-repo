from itertools import cycle
from posixpath import split
import pandas as pd
#得到去掉所有null/space值后的series
#使用isspace函数处理空值，输入series，返回去除空值后的series
def del_null(series):
    null = (series.isnull() | series.apply(lambda x:str(x).isspace()))
    mask = ~null
    return series[mask]

'''
分割函数
'''
#以\n分割
def split_n(string):
    list = string.split('\n')  #以：为分隔符，切割最前面的一次
    return list

#以,分割
def split_comma(string):
    list = string.split(',')
    return list

#以;分割
def split_semicolon(string):
    list = string.split(';')
    return list

#以=分割
def split_equals(string):
    list = string.split('=')
    return list

'''
将以\n,:分割的处理成dict
test_lists 为以\n处理过的lists
'''
def strblock2dict(test_lists):
    dict = {}
    previous_key = ''
    for i in range(len(test_lists)):
        if ':' in test_lists[i]:
            result = test_lists[i].split(':')
            dict[result[0]] = result[1]
            previous_key = result[0]
        else:
            try:
                dict[previous_key] = ' '.join((dict[previous_key],test_lists[i]))
            except:
                print(i,'keyerror')
            # test_dict[previous_key] = test_dict[previous_key] + test_lists[i]
    return dict

#对dict中的每个value进行函数处理
def apply_func_to_dict(dict,func):
    for i in dict.values():
        i.apply(func)

'''
统计检查结果
# 适用于一切以\n,:分隔的数据生成dataframe
1:先使用del_null去重
2:使用split_n分割
3:使用strblock2dict转化成dict
4:将dict按照顺序放进列表中
5:以原来的index为新df的index,将列表中字典里的key / value 作为新df的 columns 和 value
'''
def diag_test(series):
    series = del_null(series)
    diag_test_ls = series.apply(split_n)
    diag_test_dict = diag_test_ls.apply(strblock2dict)
    ls_index = series.index
    ls_value = []
    for i in diag_test_dict:
        ls_value.append(i)
    diag_test_df = pd.DataFrame(ls_value,
                                index = ls_index)
    return diag_test_df

'''
验证CT/US数据的完整性:是否有5个逗号
验证MRI数据的完整性:逗号和分号数量一致,分号隔开的是(序列,序列信号),最后一项为other findings
PET数据较少,不验证直接分析,但至少应有一个,
type为要检验的是哪种检查,如CT,data为对应的series
'''

def data_validator(type,data):
    for a,i in enumerate(data):
        if type == 'CT' or type == 'US':  # if any(type == 'US',type == 'CT')
            try:
                if i.count(',') == 5:  #CT / US的数据以5个,分割成6项
                    pass
                else:
                    print(f'iloc:{a}',i)
            except:
                print(a,i,'error')
        elif type == 'MRI':
            try:
                if i.count(',') == i.count(';'):
                    pass
                else:
                    print(f'iloc:{a}',i)
            except:
                print(a,i,'error')
        elif type == 'standard':  #standard 表示以\n和： 分隔的series，用于整理成dataframe
            try:
                if '\n' in i:
                    temp_list = i.split('\n')
                    for j in temp_list:
                        if ':' in j:
                            temp_aspect = j.split(':')
                            if temp_aspect[0] == ' ' or temp_aspect[0] == '':
                                print(a,i,'key error')
                            else:
                                pass
                        else:
                            print(a,i,'no colon')
                else:
                    if i.count(':') == 1:
                        pass
                    else:
                        print(a,i,'no sufficient spliting punctuation')
            except:
                print(a,i,'error')
        elif type == 'chemo':
            try:
                if i.count(':') == i.count('\n') + 1:
                    if ',' in i:  #如果有逗号则分号数量=逗号数量×2
                        if i.count(';') == (i.count(':') + i.count(','))*2 :
                            pass
                        else:
                            print(a,i,'wrong number of comma & semicolon：number of semicolon should be twice of comma')
                    elif i.count(';') == 2*i.count(':'):
                        pass
                    else:
                        print(f'iloc{a}',i,'wrong number of semicolon & colon')
                else:
                    print(f'iloc{a}',i,'wrong number of \\n & colon')
            except:
                print(f'iloc{a}',i,'data error')
        elif type == 'surgery':
            try:
                if i.count(',') == 3:
                    pass
                else:
                    print(f'iloc{a},{i},wrong number of comma')
            except:
                print(f'iloc{a}',i,'data error')
        else:
            pass

#处理CT / US的格式化数据
def CT(series):
    series = del_null(series)
    CT_list = series.apply(split_comma)
    ls_index = series.index
    ls_value = []
    for i in CT_list:
        ls_value.append(i)
    CT_df = pd.DataFrame(ls_value,  
                        index = ls_index,
                        columns = ['amount','border','density','enhancement','enhancement pattern','other findings'])
    return CT_df

#处理MRI数据
def MRI(series):
    series = del_null(series)
    MRI_list_with_semicolon = series.apply(split_comma)   #如果没有comma则变为单元素list
    def MRI_list_to_dict(ls):
        dic = {}
        for i in ls:
            if ';' in i:
                dic[i.split(';')[0]] = i.split(';')[1]
            else:
                dic['other findings'] = i
        return dic
    MRI_list = MRI_list_with_semicolon.apply(MRI_list_to_dict)
    ls_index = MRI_list.index
    ls_value = []
    for i in MRI_list:
        ls_value.append(i)
    MRI_df = pd.DataFrame(ls_value,
                            index = ls_index,
                            columns = ['T1WI','T2WI','T2-FLAIR','DWI','ADC','other findings'])  
    return MRI_df

#处理PET数据
#最后一位是metastatic info，前面是（位置;SUV）或者是整体的SUV
def PET(series):
    series = del_null(series)
    PET_list_split_comma = series.apply(split_comma)
    def PET_list_to_dict(ls):
        dic = {}
        dic['metastatic info'] = ls[-1]
        for i in range(len(ls)-1):
            if ';' in ls[i]:
                dic[ls[i].split(';')[0]] =ls[i].split(';')[1]
            else:
                dic['general uptake'] = ls[i]
        return dic
    PET_list = PET_list_split_comma.apply(PET_list_to_dict)
    ls_index = series.index
    ls_value = []
    for i in PET_list:
        ls_value.append(i)
    PET_df = pd.DataFrame(ls_value,index = ls_index)

def patho(series):
    series = del_null(series)
    patho_ls = series.apply(split_n)
    patho_dict = patho_ls.apply(strblock2dict)
    return patho_dict

#处理chemo
def chemo_(series):
    series = del_null(series)
    df_chemo = series.str.split('\n',expand = True)

    #先分出每个cycles分别的方案
    max_cycles = len(df_chemo.columns)
    cycles = []
    for i in range(max_cycles):
        cycles.append(f'cycle{i+1}')

    df_chemo.columns = cycles
    def rep(str):
        if str:
            str = str.replace(';','-')
            str = str.replace(',','\n')
            return str
    df_chemo = df_chemo.applymap(rep)

    #列出使用的药的种类


    df_chemo['drugs'] = 'yes'
    
    for i in df_chemo.index:
        blank_set = set([])
        _ = df_chemo.loc[i,cycles].apply(extract_drugs)
        for j in _:
            for item in j:
                blank_set.add(item)
        if len(blank_set) != 0:
            df_chemo.loc[i,'drugs'] = blank_set
    return df_chemo

#用于处理drugs的函数
def extract_drugs(str):
    drug_ls = []
    if str:
        try:
            drug_info = str.split(':')[1]
            drug_usage_list = drug_info.split('\n')
            for i in drug_usage_list:
                drug_ls.append(i.split('-')[0])
        except:
            print("ERROR")
    return drug_ls

#处理surgery
def surgery(series):
    series = del_null(series)
    surg_ls = series.apply(split_comma)
    ls_index = series.index
    ls_value = []
    for i in surg_ls:
        ls_value.append(i)
    surg_df = pd.DataFrame(ls_value,  
                        index = ls_index,
                        columns = ['extent','R/L','major operation','adjuvant operation'])
    return surg_df

def radio(series):
    series = del_null(series)
    def rep(str):
        return str.replace(':',',')
    radio_df = series.apply(rep).str.split(',',expand=True)
    radio_df.columns = ['cycles','rays','dose','time']
    return radio_df

def fill_null(str):
    if len(str) == 0:
        str = 'yes'
    return str

#处理outcome,数据验证使用‘standard类’，数据处理使用diag_test

    