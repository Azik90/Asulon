import pandas as pd
from simpledbf import Dbf5
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()  # Скрыть главное окно
print('Выбери форму Ф030 dbf')
file_path = filedialog.askopenfilename()  # Открыть диалоговое окно выбора файла
dbf = Dbf5(file_path, codec='CP866')
# dbf = Dbf5('F030A.dbf', codec='CP866')
print('Выбери отчет РЭМД csv')
file_path2 = filedialog.askopenfilename()  # Открыть диалоговое окно выбора файла
df_all = pd.read_csv(file_path2, sep=';', encoding='windows-1251')
#--------------------------------------------------------------------------
# Работаем с формой Ф-030А (dbf файл) 
df = dbf.to_dataframe()
# print(df)
data_all_num = []
dict_num = {}
numR_snils = {}
for row in df.itertuples():
    # print(row.docNum, row.emdr_id)

    docNum = str(row.SN_LR)
    data_all_num.append(docNum)

    dict_num[docNum] = [row.DATE_VR, row.PCOD]
    numR_snils[docNum] = row.SNILS

#-------------------------------------------------------------------------------
# Работаем с ответом из РЭМД (csv файл) 
data = {} 
data_none = []
for row in df_all.itertuples():
    # print(row.docNum, row.emdr_id)

    id_remd = str(row.emdr_id)
    # Есть ли такой номер рецепта уже в словаре
    if row.docNum in data:
        # Проверям. были ли ранее ответ от РЭМДа
        if id_remd == 'nan' and str(data[row.docNum]) != 'nan':
            # print(row.docNum, data[row.docNum])
            continue

    data[row.docNum] = id_remd

for key in data:
    # рецепты с ошибкой из РЭМД
    if data[key] == 'nan':
        data_none.append(key)
#--------------------------------------------------------------------------------

not_SEMD = []
for numR in data_all_num:
    # Есть ли такой номер рецепта уже в словаре (пробовали отправить в РЭМД)
    if numR in data:
        pass
    else:
        # Не был отправлен в РЭМД
        # print('Врач не подписал СЭМД (не создан), номер рецепта: ',numR, dict_num[numR][0], dict_num[numR][1])
        not_SEMD.append({'Рецепт_№':numR, 'Дата':dict_num[numR][0], 'Врач':dict_num[numR][1], 'текст':'Врач не подписал с ЭПЦ выписанный рецепт (СЭМД не сформирован)'})

df_n_semd = pd.DataFrame(data=not_SEMD)
# Не отправленые в РЭМД (не созданные)
df_n_semd.to_excel('Not_SEMD.xlsx', index=False)

#--------------------------------------------------------------------------------
# СЭМДы вернувшиеся с ошибкой из РЭМД
error_SEMD = []
for numR in data_none:
    x = df_all[df_all['docNum'] == numR]
    if 'NOT_UNIQUE_PROVIDED_ID' in x['error_id'].values:
        # Уже зареганные в РЭМД за ошибку не считаем
        continue
    text = x['error_txt'].values[-1]
    vrach = x['FIO_Signer'].values[0]
    messId = x['messId'].values[0]
    Snils_vrach = x['Snils_Signer'].values[0]
    Snils_pasient = 'нет данных о СНИЛС'
    if numR in numR_snils:
        Snils_pasient = numR_snils[numR]

    if str(text) == 'nan':
        text = 'ХЗ, что то не так, ждем ответа от РЭМД...'
        
    # print(numR, text, vrach)
    error_SEMD.append({'Рецепт_№':numR,'messId':messId,'ОШИБКА':text, 'Врач':vrach, 'Врач_снилс':Snils_vrach, 'Пациент_СНИЛС':Snils_pasient})
    
df_error_semd = pd.DataFrame(data=error_SEMD)
df_error_semd.to_excel('ERROR_SEMD.xlsx', index=False)

input('Конец выполнения скприта, нажми ENTER...')
