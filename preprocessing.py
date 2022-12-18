import pandas as pd
import numpy as np
import os


for n in range(11,13):
    if n < 10: n = '0'+str(n)
    else: n = str(n)

    # 월의 몇일이 있는지 알기 위해서 excel파일의 개수를 센다
    path = f'.\LOCAL_PEOPLE_2018{n}' 
    file_list = os.listdir(path) 

    # 월의 1일을 불러온다
    df = pd.read_csv(f'./LOCAL_PEOPLE_2018{n}/LOCAL_PEOPLE_2018{n}01.csv')#,encoding='cp949') #
    df.rename(columns={'?"기준일ID"':'기준일ID'},inplace=True) # 오타 처리

    code = pd.read_csv('행정동코드.csv',index_col=0) 
    code = code.query("시군구명 == '용산구'")

    # df.reset_index(inplace=True)
    # df = pd.DataFrame(df.iloc[:,:-5].values,columns=df.columns[5:])   
    
    
    df1 = pd.merge(df.iloc[:,:5],code.iloc[:,1:], on='행정동코드',how ='left')
    df1 = df1.dropna()
    df1 = df1[df1.apply(lambda x : x['총생활인구수'] != 0 ,axis=1)]

    # 기준일을 년과 월로 구분한다
    df1['기준일ID']= df1['기준일ID'].astype('str')
    df1['기준일ID'] = pd.to_datetime(df1['기준일ID'])
    df1['년'] = df1['기준일ID'].dt.year
    df1['월'] = df1['기준일ID'].dt.month

    # 필요없는 컬럼 제거
    df1.drop(['기준일ID'],axis=1,inplace=True)
    df1.drop(['집계구코드'],axis=1,inplace=True)



    # 시간대 구분과 행정동명을 groupby하여 총생활인구수의 평균값을 다시 총생활인구수로 묶는다
    df1['총생활인구수']= df1['총생활인구수'].astype(int) # 총생활인구수가 object로 되어있는 경우 int로 바꾸어준다.
    df1['총생활인구수'] = df1.groupby(['시간대구분','행정동명'])['총생활인구수'].transform('mean')
    df1['총생활인구수'] = df1['총생활인구수'].astype(int)

    # 중복된 값들을 제거한다.
    df1.drop_duplicates(inplace=True)

    # df1.replace('*',np.nan,inplace=True)
    # df1.dropna(inplace=True)
    # df3 = df1.groupby()['총생활인구수'].transform('mean')

    for i in range(2,len(file_list)+1):
        try :
            if i < 10: i = '0'+str(i)
            else: i = str(i)
                
            df = pd.read_csv(f'./LOCAL_PEOPLE_2018{n}/LOCAL_PEOPLE_2018{n}{i}.csv')#,encoding='cp949') #
            df.rename(columns={'?"기준일ID"':'기준일ID'},inplace=True)

            df2 = pd.merge(df.iloc[:,:5],code.iloc[:,1:], on='행정동코드',how ='left')
            df2 = df2.dropna()
            df2 = df2[df2.apply(lambda x : x['총생활인구수'] != 0 ,axis=1)]

            df2['기준일ID']= df2['기준일ID'].astype('str')

            df2['기준일ID'] = pd.to_datetime(df2['기준일ID'])

            df2['년'] = df2['기준일ID'].dt.year
            df2['월'] = df2['기준일ID'].dt.month

            df2.drop(['기준일ID'],axis=1,inplace=True)
            df2.drop(['집계구코드'],axis=1,inplace=True)

            df2['총생활인구수']= df2['총생활인구수'].astype(int)
            df2['총생활인구수'] = df2.groupby(['시간대구분','행정동명'])['총생활인구수'].transform('mean')
            df2['총생활인구수'] = df2['총생활인구수'].astype(int)

            df2.drop_duplicates(inplace=True)
            tmp = (df1['총생활인구수'].values + df2['총생활인구수'].values)//2
            print(f"LOCAL_PEOPLE_2018{n}{i}.csv를 완료했습니다.") #

        except (FileNotFoundError,ValueError) as e:
            print('error : ',e)
            continue
        
        
    # 평균값을 계산하여 최종적으로 총생활인구수에 삽입하고 저장한다.
    df1['총생활인구수'] = tmp
    df1.to_csv(f'LOCAL_PEOPLE_2018{n}.csv')
    print(f'LOCAL_PEOPLE_2018{n}.csv 생성')


########## 여러개의 month를 year 통합시키기 ##########
month = []
for i in range(1,13):
    if i < 10: i = '0'+str(i)
    else: str(i)
    df = pd.read_csv(f'LOCAL_PEOPLE_2018{i}.csv')
    month.append(df)
result = pd.concat(month)
result.to_csv('LOCAL_PEOPLE_2020.csv',index=False)
print('통합완료')