from PyQt6 import QtCore, QtGui, QtWidgets
from PyQt6.QtWidgets import QListWidgetItem
import sys
import json
import os
import datetime
from datetime import datetime, timedelta
import shutil
import hashlib
from moviepy.editor import *
import cx_Oracle
import magic
import logging


logger = logging.getLogger('main')

logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('main.log', encoding='utf-8')

fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()

ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)


dsn = cx_Oracle.makedsn(host='///', port='///', service_name='///')

try:
    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    print("База данных доступна")
    connection.close()
except cx_Oracle.Error as error:
    print("Ошибка:", error)

Department = 1
Section = 2
Group = 0
Information_type_id_for_video = 60
Information_type_id_for_audoi = 50


def get_oks():
    logger.debug('Вызов функции, которая получает из базы все ok_id\n')

    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    cursor = connection.cursor()

    oks = cursor.var(cx_Oracle.CURSOR)
    cursor.callproc("TABLES.Pack_Commutation_v3.get_OK_All", [oks])
    result_oks = oks.getvalue()

    sp_all_oks = []
    for row in result_oks:
        row = list(row[:3])
        sp_all_oks.append(row)

    cursor.close()
    connection.close()
    
    return sp_all_oks

def get_video():
    logger.debug('Вызов функции, которая получает из базы каталог для видео файла\n')

    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    cursor = connection.cursor()

    get_dir_video = cursor.var(cx_Oracle.CURSOR)
    cursor.callproc("TABLES.Pack_Control_Module.Get_Path", [Department, Section, Group, Information_type_id_for_video, get_dir_video])
    result_get_dir_video = get_dir_video.getvalue() 

    for row in result_get_dir_video:
        sp_dir_video = row[0]

    cursor.close()
    connection.close()

    return sp_dir_video

def get_audio():
    logger.debug('Вызов функции, которая получает из базы каталог для аудио файла\n')

    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    cursor = connection.cursor()

    get_dir_audio = cursor.var(cx_Oracle.CURSOR)
    cursor.callproc("TABLES.Pack_Control_Module.Get_Path", [Department, Section, Group, Information_type_id_for_audoi, get_dir_audio])
    result_get_dir_audio = get_dir_audio.getvalue()

    for row in result_get_dir_audio:
        sp_dir_audio = row[0]

    cursor.close()
    connection.close()

    return sp_dir_audio

def open_json():
    logger.debug('Вызов функции, которая открывает json файл\n')

    if os.path.exists('data.json'):
        with open('data.json', 'r') as infile:
            data_files = json.load(infile)
    else:
        data_files = [[], {}, {}]
        logger.info('Создание json файла (Json файл появится при добавлении первого каталога в список всех каталогов)\n')

    return data_files

def print_id_dir(self):
    logger.debug('Вызов функции, которая выводит в третью колонку список ok + directory\n')

    data_files = open_json()
    self.listWidget_3.clear()

    if len(data_files[2]) > 0:
        for key, value in data_files[2].items():
            text2 = key
            text1 = value
            combined_text = text1 + ' + ' + text2[:text2.find(' ')]
            new_item = QListWidgetItem(combined_text, self.listWidget_3)
            new_item.setBackground(QtGui.QColor(200, 200, 255))

    if len(data_files[1]) > 0:
        for key, value in data_files[1].items():
            text2 = key
            text1 = value
            combined_text = text1 + ' + ' + text2[:text2.find(' ')]
            new_item = QListWidgetItem(combined_text, self.listWidget_3)
            new_item.setBackground(QtGui.QColor(200, 255, 200))


def write_to_procedure(file_name, ok_id, end_data, information_type_id, file_folder, duration):
    logger.debug('Вызов функции, которая записывает в базу описание добавленного файла\n')

    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    cursor = connection.cursor()

    current_time = datetime.now()
    begin_date = current_time.strftime('%d/%m/%Y %H:%M:%S')
    direction = -1
    ok_phone_number = None
    phone_number = None
    record_type = 0
    computer_id = 1
    registration_id = 7
    storage_type = "ОХ"
    storage_doc_number = None
    removal_lock_mark = -1
    file_folder2 = None
    input_mark = -1
    language_id = -1
    status = -1
    note = None

    p_mes = cursor.var(cx_Oracle.STRING)
    p_return = cursor.var(cx_Oracle.NUMBER)
    cursor.callproc("TABLES.Pack_Control_Module.Insert_Media_File", [file_name,
                                                                    ok_id,
                                                                    begin_date,
                                                                    end_data,
                                                                    information_type_id,
                                                                    direction,
                                                                    ok_phone_number,
                                                                    phone_number,
                                                                    record_type,
                                                                    computer_id,
                                                                    registration_id,
                                                                    duration,
                                                                    storage_type,
                                                                    storage_doc_number,
                                                                    removal_lock_mark,
                                                                    file_folder,
                                                                    file_folder2,
                                                                    input_mark,
                                                                    language_id,
                                                                    status,
                                                                    note,
                                                                    p_mes,
                                                                    p_return])
    
    res = p_return.getvalue()
    logger.info(f"p_mes: {p_mes.getvalue()}")
    logger.info(f"p_return: {res}")

    cursor.close()
    connection.close()

    return res

def check_dir_proc(ok_id, file_name):
    connection = cx_Oracle.connect(user='///', password='///', dsn=dsn)
    cursor = connection.cursor()

    cursor.execute("SELECT file_name FROM TABLES.AUDIO_FRAGMENTS WHERE OK_ID = :ok_id AND file_name = :file_name", {'ok_id': ok_id, 'file_name': file_name})
    result = cursor.fetchone() is not None

    cursor.close()
    connection.close()

    return result

counter = 0
class Ui_MainWindow(QtWidgets.QMainWindow):
    def setupUi(self, MainWindow):
        self.counterAll = 0
        self.counterCur = 0
        self.counterError = 0

        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(740, 460)
        MainWindow.setWindowOpacity(1.0)

        self.centralwidget = QtWidgets.QWidget(parent=MainWindow)
        self.centralwidget.setObjectName("centralwidget")

        self.pushButton = QtWidgets.QPushButton(parent=self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(0, 380, 271, 31))
        self.pushButton.setObjectName("pushButton")

        self.pushButton_2 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.pushButton_2.setGeometry(QtCore.QRect(0, 410, 271, 23))
        self.pushButton_2.setObjectName("pushButton_2")

        self.line = QtWidgets.QFrame(parent=self.centralwidget)
        self.line.setGeometry(QtCore.QRect(270, -10, 16, 461))
        self.line.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        self.line.setFrameShadow(QtWidgets.QFrame.Shadow.Sunken)
        self.line.setObjectName("line")

        self.line_2 = QtWidgets.QFrame(parent=self.centralwidget)
        self.line_2.setGeometry(QtCore.QRect(500, 0, 16, 461))
        self.line_2.setFrameShape(QtWidgets.QFrame.Shape.VLine)
        self.line_2.setFrameShadow(QtWidgets.QFrame.Shadow.Sunken)
        self.line_2.setObjectName("line_2")

        self.label = QtWidgets.QLabel(parent=self.centralwidget)
        self.label.setGeometry(QtCore.QRect(10, 0, 261, 31))
        self.label.setObjectName("label")

        self.label_2 = QtWidgets.QLabel(parent=self.centralwidget)
        self.label_2.setGeometry(QtCore.QRect(290, 0, 211, 31))
        self.label_2.setObjectName("label_2")

        self.listWidget = QtWidgets.QListWidget(parent=self.centralwidget)
        self.listWidget.setGeometry(QtCore.QRect(0, 30, 271, 341))
        self.listWidget.setObjectName("listWidget")

        self.listWidget_2 = QtWidgets.QListWidget(parent=self.centralwidget)
        self.listWidget_2.setGeometry(QtCore.QRect(280, 30, 221, 341))
        self.listWidget_2.setObjectName("listWidget_2")

        bg_green = "200, 255, 200"
        bg_blue = "200, 200, 255"

        self.pushButton_3 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.pushButton_3.setGeometry(QtCore.QRect(520, 30, 111, 31))
        self.pushButton_3.setStyleSheet(f"background-color: rgb({bg_green});")
        self.pushButton_3.setObjectName("pushButton_3")

        self.pushButton_4 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.pushButton_4.setGeometry(QtCore.QRect(520, 70, 111, 31))
        self.pushButton_4.setStyleSheet(f"background-color: rgb({bg_blue});")
        self.pushButton_4.setObjectName("pushButton_4")
        
        self.listWidget_3 = QtWidgets.QListWidget(parent=self.centralwidget)
        self.listWidget_3.setGeometry(QtCore.QRect(520, 161, 171, 211))
        self.listWidget_3.setObjectName("listWidget_3")

        self.label_3 = QtWidgets.QLabel(parent=self.centralwidget)
        self.label_3.setGeometry(QtCore.QRect(530, 130, 161, 31))
        self.label_3.setObjectName("label_3")

        self.statusBar = QtWidgets.QStatusBar(parent=MainWindow)
        self.statusBar.setObjectName("statusBar")
        MainWindow.setStatusBar(self.statusBar)

        MainWindow.setCentralWidget(self.centralwidget)
        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)
        
        data_files = open_json()
        self.listWidget_3.clear()
        self.listWidget.clear()
        self.listWidget_2.clear()
        for i in range(len(data_files[0])):
            direct = data_files[0][i]
            self.listWidget.addItem(direct[direct.rfind('/') + 1:])

        sp_all_oks = get_oks()
        for row in sp_all_oks:
            row = list(row)
            if row[-1] == "None":
                self.listWidget_2.addItem(f"{row[0]} - {row[1]}")
            else:
                self.listWidget_2.addItem(f"{row[0]} - {row[1]} ({row[2]})")
        
        print_id_dir(self)
        self.statusBar.showMessage(f'Обработано - {self.counterCur}/{self.counterCur}. Ошибок - {self.counterError}')

        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(self.checking_a_files)
        self.countTime = 30000          # 1000 = 1 секунда
        self.timer.start(self.countTime)

        logger.info('Запуск программы и вывод текста на экран\n')


    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate

        MainWindow.setWindowTitle(_translate("MainWindow", "Soho"))

        self.pushButton.setText(_translate("MainWindow", "Add directory"))
        self.pushButton.clicked.connect(self.get_directory)

        self.pushButton_2.setText(_translate("MainWindow", "Delete directory"))
        self.pushButton_2.clicked.connect(self.delete_directory)

        self.label.setText(_translate("MainWindow", "Existing Directory:"))
        self.label_2.setText(_translate("MainWindow", "ID:"))
        
        self.pushButton_3.setText(_translate("MainWindow", "Audio"))
        self.pushButton_3.clicked.connect(self.add_green_item)

        self.pushButton_4.setText(_translate("MainWindow", "Video"))
        self.pushButton_4.clicked.connect(self.add_blue_item)

        self.label_3.setText(_translate("MainWindow", "Directory + id"))
    

    def get_directory(self):
        logger.debug('Запустилась функция get_directory\n')

        data_files = open_json()
        list_of_files = data_files[0]

        direct = QtWidgets.QFileDialog.getExistingDirectory()
        if len(direct) > 0:
            list_of_files.append(direct)

        self.listWidget.clear()
        for i in range(len(list_of_files)):
            direct = list_of_files[i]
            self.listWidget.addItem(direct[direct.rfind('/') + 1:])

        logger.info(f'Добавление каталога в список файлов {direct}\n')

        data_files[0] = list_of_files
        with open('data.json', 'w') as outfile:
            json.dump(data_files, outfile)
        
        logger.debug('Сохранились изменения и вывелось на экран\n')


    def delete_directory(self):
        logger.debug('Запустилась функция delete_directory\n')

        data_files = open_json()
        list_of_files = data_files[0]
        audio_files = data_files[1]
        video_files = data_files[2]

        try:
            row = self.listWidget.currentRow()
            if row != -1:
                self.listWidget.takeItem(row)
            deldir = list_of_files[row]
            
            del list_of_files[row]

            logger.info(f'Удалился файл из списка Existing Directory - {deldir}\n')

            for _ in range(len(video_files)):
                for key, value in video_files.items():
                    if value == deldir[deldir.rfind('/') + 1:]:
                        del video_files[key]
                        break
            
            for _ in range(len(audio_files)):
                for key, value in audio_files.items():
                    if value == deldir[deldir.rfind('/') + 1:]:
                        del audio_files[key]
                        break
            
            logger.info('Удалились папки из json файла и из id + directory списка\n')
            
            data_files[1] = audio_files
            data_files[2] = video_files
            data_files[0] = list_of_files
            with open('data.json', 'w') as outfile:
                json.dump(data_files, outfile)
            
            print_id_dir(self)

            logger.debug('Сохранились изменения и вывелось на экран\n')

        except:
            pass
            logger.warning('''Произашла ошибка в delete_directory при удалении папки из списка.
Возможно пользователь пытался удалить пустой список.''')


    def add_green_item(self):
        logger.debug('Соединение каталога и ok-id. Соединение аудио дорожки.\n')

        data_files = open_json()
        list_of_files = data_files[0]
        audio_files = data_files[1]
        video_files = data_files[2]


        selected1 = self.listWidget.selectedItems()
        selected2 = self.listWidget_2.selectedItems()

        logger.info('Создание строк в аудио дорожке\n')
        if len(selected1) == 1 and len(selected2) == 1:
            text1 = selected1[0].text()
            text2 = selected2[0].text()
            combined_text = text1 + ' + ' + text2[:text2.find(' ')]

            count = self.listWidget_3.count()
            for i in range(count):
                item = self.listWidget_3.item(i)
                try:
                    logger.info('1.Удаление старых строк в соединении аудио дорожки (из двух)\n')

                    del video_files[text2]
                    data_files[2] = video_files
                    with open('data.json', 'w') as outfile:
                        json.dump(data_files, outfile)

                except:
                    logger.warning(f'Возможно незначительная ошибка при соединении/удалении каталога и ok-id для аудио дорожки {text1} - {text2}\n')

                if item is not None and item.text() == combined_text:
                    old_item = self.listWidget_3.takeItem(i)
                    old_item = None
            
            logger.info('2.Удаление старых строк в соединении аудио дорожки\n')

            for k, v in list(audio_files.items()):
                if v == text1:
                    del audio_files[k]

            for k, v in list(video_files.items()):
                if v == text1:
                    del video_files[k]

            audio_files[text2] = text1
            data_files[1] = audio_files
            with open('data.json', 'w') as outfile:
                json.dump(data_files, outfile)

            print_id_dir(self)

            logger.debug(f'Сохранились изменения и вывелось на экран: {text1} - {text2}\n')


    def add_blue_item(self):
        logger.debug('Соединение каталога и ok-id. Соединение видео дорожки.\n')

        data_files = open_json()
        list_of_files = data_files[0]
        audio_files = data_files[1]
        video_files = data_files[2]

        selected1 = self.listWidget.selectedItems()
        selected2 = self.listWidget_2.selectedItems()

        logger.info('Создание строк в видео дорожке\n')
        if len(selected1) == 1 and len(selected2) == 1:
            text1 = selected1[0].text()
            text2 = selected2[0].text()
            combined_text = text1 + ' + ' + text2[:text2.find(' ')]

            count = self.listWidget_3.count()
            for i in range(count):
                item = self.listWidget_3.item(i)
                try:
                    logger.info('1.Удаление старых строк в соединении видео дорожки (из двух)\n')
                    del audio_files[text2]
                    video_files.remove(text1)
                    data_files[1] = audio_files
                    with open('data.json', 'w') as outfile:
                        json.dump(data_files, outfile)

                except:
                    logger.warning(f'Возможно незначительная ошибка при соединении/удалении каталога и ok-id для видео дорожки {text1} - {text2}\n')
                
                if item is not None and item.text() == combined_text:
                    old_item = self.listWidget_3.takeItem(i)
                    old_item = None
            
            for k, v in list(video_files.items()):
                if v == text1:
                    del video_files[k]

            for k, v in list(audio_files.items()):
                if v == text1:
                    del audio_files[k]

            logger.info('2.Удаление старых строк в соединении видео дорожки\n')

            video_files[text2] = text1
            data_files[2] = video_files
            with open('data.json', 'w') as outfile:
                json.dump(data_files, outfile)

            print_id_dir(self)

            logger.debug(f'Сохранились изменения и вывелось на экран: {text1} - {text2}\n')

    def checking_a_files(self):
        logger.debug(f'Происходит проверка файлов и передача в сетевую папку (Каждые {self.countTime} секунд)\n')

        year = str(datetime.now().year)
        month = str(datetime.now().month)
        day = str(datetime.now().day)

        def start(destination, des_files, list_of_files, only_audio):
            logger.debug('Вызов функции, которая начинает перекидывание файлов, если файлы какие то есть в папках\n')

            try:
            #if True:
                for k, v in des_files.items():
                    logger.debug('Распаковка элементов из словаря\n')

                    ok_id = k[:k.find(' ')]
                    for i in list_of_files:
                        if i[i.rfind('/') + 1:] == v:
                            break
                    need_dir = i

                    try:
                        files = os.listdir(need_dir)
                    except:
                        logger.warning(f'Пути "{need_dir}" не существует.')
                        continue

                    logger.debug('Добавление нужных элементов в список (аудио и видео файлы из основного каталога)\n')
                    sp = []
                    for k in files:
                        if os.path.isfile(need_dir + '/' + k):
                            mime = magic.Magic(mime=True)
                            file_type = mime.from_file(need_dir + '/' + k)
                            if file_type.startswith('audio'):
                                sp.append(k)

                            if file_type.startswith('video'):
                                sp.append(k)
                    self.counterAll += len(sp)

                    for j in sp:
                        try:
                            logger.debug('Проверка, какая функция будет выполняться(на проверку видео или аудио файлов)\n')

                            if only_audio == 50:
                                logger.debug('''Проверка аудио файлов, идет проверка какая функция будет выполняться
                                             (на передачу видео или аудио файлов)\n''')
                                mime = magic.Magic(mime=True)
                                file_type = mime.from_file(need_dir + '/' + j)
                                if file_type.startswith('audio'):
                                    logger.debug('Передача аудио в аудио дорожке')
                                    logger.debug('Идет создание директорий (id/год/месяц/день/файл)\n')

                                    if not os.path.isdir(destination + ok_id):
                                        os.mkdir(f'{destination}/{ok_id}')
                                    new_destination = destination + ok_id + '/' 

                                    logger.debug('Идет создание директорий (год/месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + year):
                                        os.mkdir(f'{new_destination}/{year}')
                                    new_destination += year + '/'

                                    logger.debug('Идет создание директорий (месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + month):
                                        os.mkdir(f'{new_destination}/{month}')
                                    new_destination += month + '/'

                                    logger.debug('Идет создание директорий (день/файл)\n')
                                    if not os.path.isdir(new_destination + day):
                                        os.mkdir(f'{new_destination}/{day}')
                                    new_destination += day + '/'

                                    logger.debug(f'Копирование файла: {need_dir}/{j} - {new_destination + j}\n')

                                    ch_dr_p = check_dir_proc(ok_id, j)
                                    if ch_dr_p or os.path.isfile(new_destination + j):
                                        ndj = need_dir + '/' + j
                                        logger.error(f'Файл "{ndj}" не передался, потому что файл с таким названием уже существует.')

                                        if not os.path.isdir('dublicate'):
                                            os.makedirs('dublicate')

                                        t_dir = os.getcwd()

                                        shutil.copy2(need_dir + '/' + j, t_dir + '/' + 'dublicate/' + j)
                                        os.remove(need_dir + '/' + j)
                                        self.counterError += 1
                                        self.counterCur += 1
                                        continue

                                    else:
                                        shutil.copy2(need_dir + '/' + j, new_destination + j)


                                    if os.path.isfile(new_destination + j):
                                        logger.debug(f'Проверка хеша\n')

                                        with open(need_dir + '/' + j, 'rb') as file_to_check1:
                                            data1 = file_to_check1.read()
                                            hash1 = hashlib.md5(data1).hexdigest()

                                        with open(new_destination + j, 'rb') as file_to_check2:
                                            data2 = file_to_check2.read()
                                            hash2 = hashlib.md5(data2).hexdigest()

                                        if hash1 == hash2:
                                            logger.debug(f'Хеш проверен, теперь удаление файла{need_dir}/{j}\n')

                                            os.remove(need_dir + '/' + j)

                                            clip = AudioFileClip(new_destination + j)
                                            duration = clip.duration
                                            duration = int(duration)
                                            clip.close()
                                            current_time = datetime.now()
                                            new_time = current_time + timedelta(seconds=duration)
                                            end_time = new_time.strftime('%d/%m/%Y %H:%M:%S')
                                            a = write_to_procedure(j, ok_id, end_time, only_audio, new_destination, duration)
                                            self.counterCur += 1

                                            if int(a) != 1:
                                                logger.error(f'Файл "{new_destination + j}" не передался.')

                                                t_dir = os.getcwd()
                                                if not os.path.isdir('warning_fragments'):
                                                    os.mkdir('warning_fragments')

                                                shutil.copy2(new_destination + j, t_dir + '/warning_fragments/' + j)
                                                os.remove(new_destination + j)
                                                self.counterError += 1
                                        
                                        else:
                                            logger.debug(f'Ошибка при проверке хеша файла {j}\n')
                                            os.remove(new_destination + j)
                                            self.counterError += 1
                                            self.counterCur += 1

                                if file_type.startswith('video'):
                                    logger.debug('Передача видео в аудио дорожке')
                                    logger.debug('Идет создание директорий (id/год/месяц/день/файл)\n')

                                    if not os.path.isdir(destination + ok_id):
                                        os.mkdir(f'{destination}/{ok_id}')
                                    new_destination = destination + ok_id + '/'

                                    logger.debug('Идет создание директорий (год/месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + year):
                                        os.mkdir(f'{new_destination}/{year}')
                                    new_destination += year + '/'

                                    logger.debug('Идет создание директорий (месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + month):
                                        os.mkdir(f'{new_destination}/{month}')
                                    new_destination += month + '/'

                                    logger.debug('Идет создание директорий (день/файл)\n')
                                    if not os.path.isdir(new_destination + day):
                                        os.mkdir(f'{new_destination}/{day}')
                                    new_destination += day + '/'

                                    logger.debug(f'Копирование файла: {need_dir}/{j} - {new_destination + j}\n')

                                    ch_dr_p = check_dir_proc(ok_id, j + '.wav')
                                    if ch_dr_p or os.path.isfile(new_destination + j + '.wav'):
                                        ndj = need_dir + '/' + j
                                        logger.error(f'Файл "{ndj}" не передался, потому что файл с таким названием уже существует.')

                                        if not os.path.isdir('dublicate'):
                                            os.makedirs('dublicate')

                                        t_dir = os.getcwd()

                                        shutil.copy2(need_dir + '/' + j, t_dir + '/' + 'dublicate/' + j)
                                        os.remove(need_dir + '/' + j)
                                        self.counterError += 1
                                        self.counterCur += 1
                                        continue

                                    else:
                                        shutil.copy2(need_dir + '/' + j, new_destination + j)

                                    if os.path.isfile(new_destination + j):
                                        logger.debug(f'Проверка хеша\n')

                                        with open(need_dir + '/' + j, 'rb') as file_to_check1:
                                            data1 = file_to_check1.read()
                                            hash1 = hashlib.md5(data1).hexdigest()

                                        with open(new_destination + j, 'rb') as file_to_check2:
                                            data2 = file_to_check2.read()
                                            hash2 = hashlib.md5(data2).hexdigest()

                                        if hash1 == hash2:
                                            logger.debug(f'Хеш проверен, теперь удаление файла{need_dir}/{j}\n')

                                            os.remove(need_dir + '/' + j)

                                            logger.debug(f'Форматирование файла в .wav - {new_destination + j}.wav\n')
                                            video = VideoFileClip(new_destination + j)
                                            video.audio.write_audiofile(f"{new_destination + j}.wav")
                                            video.close()
                                            logger.debug(f'Форматирование файла успешно\n')

                                            if os.path.getsize(f"{new_destination + j}.wav") > 0:
                                                logger.debug(f'Удаление файла{need_dir}/{j}\n')

                                                os.remove(new_destination + j)

                                                clip = AudioFileClip(f"{new_destination + j}.wav")
                                                duration = clip.duration
                                                duration = int(duration)
                                                clip.close()
                                                current_time = datetime.now()
                                                new_time = current_time + timedelta(seconds=duration)
                                                end_time = new_time.strftime('%d/%m/%Y %H:%M:%S')
                                                new_file = f'{j}.wav'
                                                
                                                a = write_to_procedure(new_file, ok_id, end_time, only_audio, new_destination, duration)
                                                self.counterCur += 1
                                                
                                                if int(a) != 1:
                                                    logger.error(f'{new_destination + j}.wav не передался.')

                                                    t_dir = os.getcwd()
                                                    if not os.path.isdir('warning_fragments'):
                                                        os.mkdir('warning_fragments')
                                                    shutil.copy2(f'{new_destination + j}.wav', f'{t_dir}/warning_fragments/{j}.wav')
                                                    os.remove(f'{new_destination + j}.wav')
                                                    self.counterError += 1
                                        
                                        else:
                                            logger.debug(f'Ошибка при проверке хеша файла {j}\n')
                                            os.remove(new_destination + j)
                                            self.counterError += 1
                                            self.counterCur += 1
                                            

                            if only_audio == 60:
                                logger.debug('''Проверка видео файлов. Выполняется поиск видео в списке(перепроверка)\n''')
                                mime = magic.Magic(mime=True)
                                file_type = mime.from_file(need_dir + '/' + j)
                                if file_type.startswith('video'):
                                    logger.debug('Передача видео в видео дорожке')
                                    logger.debug('Идет создание директорий (id/год/месяц/день/файл)\n')

                                    if not os.path.isdir(destination + ok_id):
                                        os.mkdir(f'{destination}/{ok_id}')
                                    new_destination = destination + ok_id + '/'

                                    logger.debug('Идет создание директорий (год/месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + year):
                                        os.mkdir(f'{new_destination}/{year}')
                                    new_destination += year + '/'

                                    logger.debug('Идет создание директорий (месяц/день/файл)\n')
                                    if not os.path.isdir(new_destination + month):
                                        os.mkdir(f'{new_destination}/{month}')
                                    new_destination += month + '/'

                                    logger.debug('Идет создание директорий (день/файл)\n')
                                    if not os.path.isdir(new_destination + day):
                                        os.mkdir(f'{new_destination}/{day}')
                                    new_destination += day + '/'

                                    logger.debug(f'Копирование файла: {need_dir}/{j} - {new_destination + j}\n')

                                    ch_dr_p = check_dir_proc(ok_id, j)
                                    if ch_dr_p or os.path.isfile(new_destination + j):
                                        ndj = need_dir + '/' + j
                                        logger.error(f'Файл "{ndj}" не передался, потому что файл с таким названием уже существует.')

                                        if not os.path.isdir('dublicate'):
                                            os.makedirs('dublicate')

                                        t_dir = os.getcwd()

                                        shutil.copy2(need_dir + '/' + j, t_dir + '/' + 'dublicate/' + j)
                                        os.remove(need_dir + '/' + j)
                                        self.counterError += 1
                                        self.counterCur += 1
                                        continue

                                    else:
                                        shutil.copy2(need_dir + '/' + j, new_destination + j)

                                    if os.path.isfile(new_destination + j):
                                        logger.debug(f'Проверка хеша\n')

                                        with open(need_dir + '/' + j, 'rb') as file_to_check1:
                                            data1 = file_to_check1.read()
                                            hash1 = hashlib.md5(data1).hexdigest()

                                        with open(new_destination + j, 'rb') as file_to_check2:
                                            data2 = file_to_check2.read()
                                            hash2 = hashlib.md5(data2).hexdigest()

                                        if hash1 == hash2:
                                            logger.debug(f'Хеш проверен, теперь удаление файла{need_dir}/{j}\n')
                                            os.remove(need_dir + '/' + j)

                                            clip = VideoFileClip(new_destination + j)
                                            duration = clip.duration
                                            duration = int(duration)
                                            clip.close()
                                            current_time = datetime.now()
                                            new_time = current_time + timedelta(seconds=duration)
                                            end_time = new_time.strftime('%d/%m/%Y %H:%M:%S')
                                            a = write_to_procedure(j, ok_id, end_time, only_audio, new_destination, duration)
                                            self.counterCur += 1

                                            if int(a) != 1:
                                                logger.error(f'{new_destination + j} не передался.')

                                                t_dir = os.getcwd()
                                                if not os.path.isdir('warning_fragments'):
                                                    os.mkdir('warning_fragments')
                                                shutil.copy2(new_destination + j, t_dir + '/warning_fragments/' + j)
                                                os.remove(new_destination + j)
                                                self.counterError += 1

                                        else:
                                            logger.debug(f'Ошибка при проверке хеша файла {j}\n')
                                            os.remove(new_destination + j)
                                            self.counterError += 1
                                            self.counterCur += 1

                        except:
                            logger.error(f'Ошибка при передачи в сетевую папку (файл - {j})')

                            self.counterError += 1

            except:
                logger.error('Ошибка при проверки файлов')


        dir_get_audio = get_audio() + '/'
        dir_get_video = get_video() + '/'

        data_files = open_json()
        list_of_files = data_files[0]
        audio_files = data_files[1]
        video_files = data_files[2]

        if len(audio_files) > 0 or len(video_files) > 0:
            dialog = QtWidgets.QMessageBox(self)
            dialog.setWindowTitle('Идет Обработка Файлов')
            dialog.setText('')
            dialog.setStandardButtons(QtWidgets.QMessageBox.StandardButton.NoButton)
            dialog.show()
            only_audio = 50
            if len(audio_files) > 0:
                start(dir_get_audio, audio_files, list_of_files, only_audio)
                logger.debug('Вызов функции для аудио дорожки\n')

            only_audio = 60
            if len(video_files) > 0:
                start(dir_get_video, video_files, list_of_files, only_audio)
                logger.debug('Вызов функции для видео дорожки\n')
        
            dialog.done(0)

            self.statusBar.showMessage(f'Обработано - {self.counterCur}/{self.counterCur}. Ошибок - {self.counterError}.')


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()

    app.exec()