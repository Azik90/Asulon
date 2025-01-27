import pandas as pd
from simpledbf import Dbf5
from datetime import datetime
import PySimpleGUI as sg

# Создаем макет окна
layout = [[sg.Text('ВВЕДИТЕ ГОД ')],
            [sg.InputText()],
        [sg.Text('Выбери ExpVipSEMD_All dbf')],
          [sg.Input(), sg.FileBrowse()],
        [sg.Text('Выбери форму F 030A dbf')],
          [sg.Input(), sg.FileBrowse()],
        [sg.Text('Выбери отчет РЭМД csv')],
          [sg.Input(), sg.FileBrowse()],
          [sg.Button('OK'), sg.Button('Cancel')]]

# Создаем окно
window = sg.Window('Выбор файла', layout)

# События в цикле обработки событий
event, values = window.read()

# Закрываем окно
window.close()
file_path1 = ''
file_path2 = ''
file_path3 = ''
d_year = ''
# Проверяем, была ли нажата кнопка "OK"
if event == 'OK':
    d_year = values[0]  # Получаем выбранный файл
    file_path1 = values[1]  # Получаем выбранный файл
    file_path2 = values[2]  # Получаем выбранный файл
    file_path3 = values[3]  # Получаем выбранный файл
    # ПРОВЕРКА НА ЦИФРЫ В СТРОКЕ
    if d_year.isdigit() == False:
        sg.popup('ВЫ ВВЕЛИ НЕПРАВИЛЬНО ГОД!\n\nГОД ДОЛЖЕН СОДЕРЖАТЬ ТОЛЬКО ЦИФРЫ!\n\nВЫХОД')
        exit()
    elif d_year not in ['2023', '2024', '2025', '2026', '2027', '2028', '2029']:
        sg.popup('ВЫ ВВЕЛИ НЕПРАВИЛЬНО ГОД!\n\nВЫХОД')
        exit()
    d_year = int(d_year) # теперь год переведем в int значение
    if (d_year and file_path1 and file_path2 and file_path3) == '':
        print('  Не все файлы указаны ...')
        sg.popup('Не все файлы указаны !\n\nВЫХОД')
        exit()

else:
    print('Выбор файлов отменен')
    exit()
#----------------------------------------------------------------------------------
print(' Начало чтения большого файла   Время: ', datetime.now())
dbf7 = Dbf5(file_path1, codec='CP866')
df7 = dbf7.to_dataframe()
# Преобразуем колонку DATE в формат datetime, если это еще не сделано
df7['DATE'] = pd.to_datetime(df7['DATE'], errors='coerce')
df_2024 = df7[df7['DATE'].dt.year == d_year]

# Создаем новый DataFrame, фильтруя строки с фразой "успешно" в 'MSG'
df7 = df_2024[df_2024['MSG'].str.contains("успешно", na=False)]

# Создаем новый DataFrame, фильтруя строки с фразой "ЕСКЛП" в 'MSG'
df_esklp = df_2024[df_2024['MSG'].str.contains("ЕСКЛП", na=False)]
data_esklp = set() # номера рецептов, где возникла ошибка ЕСКПЛ
del df_2024 # высвободим из ОЗУ

for row in df_esklp.itertuples():
    num = ''
    if 'ЕСКЛП' in row.MSG:
        text = (row.MSG).split('.')
        text2 = text[0].split(' ')
        num = text2[1]+' '+text2[2]

        data_esklp.add(num)

data_esklp = list(data_esklp)

# F030A
dbf = Dbf5(file_path2, codec='CP866')

# Протолок отправки в РЭМД
df_all = pd.read_csv(file_path3, sep=';', encoding='windows-1251')
print()
print('  Ждите, идет обработка файлов ...')
print()
# --------------------------------------------------------------------------
# Работаем с формой Ф-030А (dbf файл)
print(' Начало с формой F030', datetime.now())
print()
df = dbf.to_dataframe()
# print(df)
data_all_num = [] # номера всех рецептов за год
dict_num = {}  # для связки номера рецепта с датой выписки и врачем
numR_snils = {}  # для связки номера рецепта со СНИЛС пацента

for row in df.itertuples():

    docNum = str(row.SN_LR)
    data_all_num.append(docNum)

    dict_num[docNum] = [row.DATE_VR, row.PCOD]

    # ключи номеров рецептов запишем в словарь без пробелов
    docNum = docNum.replace(' ', '')
    numR_snils[docNum] = row.SNILS
# удалим дубликаты номеров
data_all_num = list(set(data_all_num))
# -------------------------------------------------------------------------------
# Работаем с ответом из РЭМД (csv файл)
print(' Начало с csv файлом, создание словарей   Время: ', datetime.now())
print()
data = {}
data_error = []
data_reg = []

for row in df_all.itertuples(index=False):  # index=False для экономии памяти
    # print(row)
    # exit()
    id_remd = str(row._4)

    # Если id_remd 'nan', устанавливаем его в None для удобства проверки
    if id_remd == 'nan':
        id_remd = None

    # Если номер рецепта уже есть в словаре
    previous_id = data.get(row._1)
    if previous_id is not None and previous_id is not None:  # если уже есть и не 'nan'
        continue

    # Сохраняем значение в словаре
    data[row._1] = id_remd

# Разделяем ключи на те, что с ошибками, и зарегистрированные
data_error = [key for key, value in data.items() if value is None]
data_reg = [key for key, value in data.items() if value is not None]


# --------------------------------------------------------------------------------
print(' Количество рецептов подлежащих обработке: ', len(data_all_num))
print()
print(' Начало поиска несозданных СЭМД (самая большая итерация)   Время: ', datetime.now())
print()
# Фильтруем df7 заранее, чтобы не проверять date в каждой итерации
df7_filtered = df7[df7['DATE'].dt.year == d_year]

not_SEMD = []
set_data = set(data)  # Для O(1) проверки наличия
i = 1 # счетчик
for numR in data_all_num:
    if numR not in set_data:  # Проверяем, есть ли номер рецепта в словаре
        num7 = numR[4:]
        text7 = f"{num7}.xml успешно подписан"

        # Фильтруем строки, содержащие нужный текст
        matching_rows = df7_filtered[df7_filtered['MSG'].str.contains(text7, na=False)]

        ExpVipSEMD_All = matching_rows[['DATE', 'TIME', 'MSG']].copy()

        if ExpVipSEMD_All.empty:  # Не был отправлен в РЭМД
            text_s = 'Врач не подписал с ЭПЦ выписанный рецепт (СЭМД не сформирован)'

            if numR in data_esklp:
                text_s = 'Врач пытался подписать рецепт, но возникла ошибка связанная с ЕСКЛП. Напиши в ТП (СЭМД не сформирован)'
            not_SEMD.append({
                'Рецепт_№': numR,
                'Дата': dict_num[numR][0],
                'Врач': dict_num[numR][1],
                'текст': text_s
            })
        else:  # Врач подписал
            last_record = ExpVipSEMD_All.iloc[-1]
            MSG1 = f"{last_record['MSG']} в процессе отправки в РИП СУИЗ ! Если последняя дата подписания больше 3-х дней - напиши в ТП"
            not_SEMD.append({
                'Рецепт_№': numR,
                'Дата': dict_num[numR][0],
                'Врач': dict_num[numR][1],
                'текст': MSG1,
                'ДАТА последней подписи': last_record['DATE'],
                'Время': last_record['TIME']
            })
    if i % 1000 == 0:
        print(f'  Обработано  {i}  рецептов ...')
    i += 1

# Получаем текущее время
now = datetime.now()
# Форматируем строку в нужном формате
text_d = f"{now.month:02}_{now.day:02}_{now.hour:02}_{now.minute:02}"

df_n_semd = pd.DataFrame(data=not_SEMD)
df_n_semd.to_excel(f'Not_SEMD_{text_d}.xlsx', index=False)
print()
# --------------------------------------------------------------------------------
# СЭМДы вернувшиеся с ошибкой из РЭМД
print(' Начало обработки СЭМД вернувшиеся с ошибкой из РЭМД   Время: ', datetime.now())
print()
error_SEMD = []
i = 1 # счетчик
for numR in data_error:

    if i % 100 == 0:
        print(f'  Обработано  {i}  рецептов ...')
    i += 1

    x = df_all[df_all['Серия и номер рецепта'] == numR]
    if ('NOT_UNIQUE_PROVIDED_ID' in x['Статус отправки'].values[-1]) or ('success' in x['Статус отправки'].values[-1]):
        # Уже зареганные в РЭМД за ошибку не считаем
        data_reg.append(numR)
        continue
    text = x['Статус отправки'].values[-1]
    vrach = x['ФИО автора'].values[-1]
    messId = x['Локальный идентификатор'].values[-1]
    Snils_vrach = x['СНИЛС автора'].values[-1]
    Snils_pasient = 'нет данных о СНИЛС'
    date = 'Нет информации о дате'

    n = numR.replace(' ', '')
    if n in numR_snils:
        Snils_pasient = numR_snils[n]
        date = (dict_num[numR])[0]

    if str(text) == 'nan':
        text = 'РИП СУИЗ не вернул ответ РЭМДа в АСУЛОН. Пиши в техподдержку, Если с даты отправки СЭМД прошло более 4-х дней '
    
    error_SEMD.append({'Рецепт_№': numR, 'ДАТА рецепта': date, 'ОШИБКА': text, 'Врач': vrach, 'Врач СНИЛС': Snils_vrach,
                       'Пациент СНИЛС': Snils_pasient, 'messId': messId,})


df_error_semd = pd.DataFrame(data=error_SEMD)
df_error_semd.to_excel(f'ERROR_SEMD_{text_d}.xlsx', index=False)

# отсортируем список зарегистрированных
data_reg.sort()
df_reg_semd = pd.DataFrame(data=data_reg, columns=['docNum'])
df_reg_semd.to_excel(f'REG_SEMD_{text_d}.xlsx', index=False)
print()
p = round(100*len(data_reg)/len(data_all_num), 2)
print(f'Процент успешно зарегистрированных рецептов:  {p}  %     Время: ', datetime.now())
print()
sg.popup(f'Процент успешно зарегистрированных рецептов:  {p}  % \n\nУСПЕХ!   Конец выполнения программ!\n\nВЫХОД')
