import pandas as pd
from simpledbf import Dbf5
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()  # Скрыть главное окно
print('Выбери форму F 030A dbf')
file_path = filedialog.askopenfilename()  # Открыть диалоговое окно выбора файла
dbf = Dbf5(file_path, codec='CP866')

print('Выбери отчет РЭМД csv')
file_path2 = filedialog.askopenfilename()  # Открыть диалоговое окно выбора файла
df_all = pd.read_csv(file_path2, sep=';', encoding='windows-1251')

print('Выбери ExpVipSEMD_All dbf')
file_path = filedialog.askopenfilename()  # Открыть диалоговое окно выбора файла
dbf7 = Dbf5(file_path, codec='CP866')
df7 = dbf7.to_dataframe()
print()
print('  Ждите пару минут, идет обработка файлов ...')
print()
#--------------------------------------------------------------------------
# Работаем с формой Ф-030А (dbf файл) 
df = dbf.to_dataframe()
# print(df)
data_all_num = []
dict_num = {} # для связки номера рецепта с датой выписки и врачем
numR_snils = {} #для связки номера рецепта со СНИЛС пацента

for row in df.itertuples():
    # print(row.docNum, row.emdr_id)

    docNum = str(row.SN_LR)
    data_all_num.append(docNum)

    dict_num[docNum] = [row.DATE_VR, row.PCOD]

    # ключи номеров рецептов запишем в словарь без пробелов
    docNum = docNum.replace(' ','')
    numR_snils[docNum] = row.SNILS
#-------------------------------------------------------------------------------
# Работаем с ответом из РЭМД (csv файл) 
data = {} 
data_error = []
data_reg =[]
for row in df_all.itertuples():
    # print(row.docNum, row.emdr_id)

    id_remd = str(row.emdr_id)
    # Есть ли такой номер рецепта уже в словаре
    if row.docNum in data:
        # Проверям. Были ли ранее ответ от РЭМДа, если был то ничего не сохраняем - переходим к следующей итерации
        if id_remd == 'nan' and str(data[row.docNum]) != 'nan':
            # print(row.docNum, data[row.docNum])
            continue

    data[row.docNum] = id_remd  # ключ - номер рецепта, значение - рег.номер РЭМД

for key in data:
    # рецепты с ошибкой из РЭМД
    if data[key] == 'nan':
        data_error.append(key)

    # рецепты зарегистрированные в РЭМД
    else:
        data_reg.append(key)

#--------------------------------------------------------------------------------

not_SEMD = []
for numR in data_all_num:
    # Есть ли такой номер рецепта уже в словаре (пробовали отправить в РЭМД)
    if numR in data:
        pass
    else:
        # Не был отправлен в РЭМД
        ExpVipSEMD_All = []
        # сначала проверим подпись врача
        num7 = numR[4:]

        text7 = num7 + '.xml успешно подписан'
        for row in df7.itertuples(): # df7 датафрейм файла ExpVipSEMD_All (Протокол создания СЭМД)

            if row.DATE.year != 2024:
                continue

            if text7 in row.MSG:

                text = (row.MSG).split('.')
                text2 = text[0].split('_')
                num = text2[0] + text2[1]

                ExpVipSEMD_All.append({'DATE': row.DATE, 'TIME': row.TIME, 'MSG': row.MSG, 'НОМЕР РЕЦЕПТА': num, })

        if ExpVipSEMD_All == []: # список пустой, врач не подписывал
            not_SEMD.append({'Рецепт_№':numR, 'Дата':dict_num[numR][0], 'Врач':dict_num[numR][1], 'текст':'Врач не подписал с ЭПЦ выписанный рецепт (СЭМД не сформирован)'})
        else: # список не пустой, врач подписал. Но Асулон в РИП не отправил!
            time1 = ExpVipSEMD_All[-1]['TIME']
            date1 = ExpVipSEMD_All[-1]['DATE']
            MSG1 = ExpVipSEMD_All[-1]['MSG'] + ' в процессе отправки в РИП СУИЗ ! Если последняя дата подписания больше 3-х дней - напиши в ТП'
            not_SEMD.append({'Рецепт_№': numR, 'Дата': dict_num[numR][0], 'Врач': dict_num[numR][1], 'текст': MSG1, 'ДАТА последней подписи': date1, 'Время': time1})

df_n_semd = pd.DataFrame(data=not_SEMD)
# Не отправленые в РЭМД (не созданные)
df_n_semd.to_excel('Not_SEMD.xlsx', index=False)

#--------------------------------------------------------------------------------
# СЭМДы вернувшиеся с ошибкой из РЭМД
error_SEMD = []
for numR in data_error:
    x = df_all[df_all['docNum'] == numR]
    if 'NOT_UNIQUE_PROVIDED_ID' in x['error_id'].values:
        # Уже зареганные в РЭМД за ошибку не считаем
        data_reg.append(numR)
        continue
    text = x['error_txt'].values[-1]
    vrach = x['FIO_Signer'].values[-1]
    messId = x['messId'].values[-1]
    Snils_vrach = x['Snils_Signer'].values[-1]
    Snils_pasient = 'нет данных о СНИЛС'
    date = 'Нет информации о дате'

    n = numR.replace(' ','')
    if n in numR_snils:
        Snils_pasient = numR_snils[n]
        date = (dict_num[numR])[0]

    if str(text) == 'nan':
        text = 'РИП СУИЗ не вернул ответ РЭМДа в АСУЛОН. Пиши в техподдержку, Если с даты отправки СЭМД прошло более 4-х дней '

    error_SEMD.append({'Рецепт_№':numR,'messId':messId,'ОШИБКА':text, 'Врач':vrach, 'Врач СНИЛС':Snils_vrach, 'Пациент СНИЛС':Snils_pasient, 'ДАТА рецепта':date})
    
df_error_semd = pd.DataFrame(data=error_SEMD)
df_error_semd.to_excel('ERROR_SEMD.xlsx', index=False)

# отсортируем список зарегистрированных
data_reg.sort()
df_reg_semd = pd.DataFrame(data=data_reg)
df_reg_semd.to_excel('REG_SEMD.xlsx', index=False)
input('Конец выполнения скрипта, нажми ENTER...')
