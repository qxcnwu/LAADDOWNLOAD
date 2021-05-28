# -*- coding: utf-8 -*-
# author QXC NWU
# TIME 2021/5/28

from tqdm import tqdm
import os
import threading
import ssl
from urllib.request import urlopen, Request
import argparse
import sys

def read_csv(csv_path: str, save_dir: str) -> [list, list]:
    """
    # 创建文件名称池文件大小池
    :param csv_path: csv文件路径 默认格式 文件名：日期：文件大小
    :param save_dir: 保存路径
    :return: filename_list,save_list
    """
    filename_list = []
    save_list = []
    fd = open(csv_path)
    lines = fd.readlines()
    for line in lines[1:-1]:
        filename_list.append(line.split(',')[0])
        save_list.append(os.path.join(save_dir, filename_list[-1]))
    fd.close()
    return filename_list, save_list

def get_csv(csv_path:str,url:str,token:str):
    """
    # 下载csv函数
    :param csv_path: 文件名
    :param url: URL
    :param token: token
    :return: null
    """
    # URL修改位置
    url = url+'.csv'
    # Token修改位置 Bearer
    headers = {
        'user-agent': 'tis/download.py_1.0--3.7.6rc1 (tags/v3.7.6rc1:bd18254b91, Dec 11 2019, 20:31:07) [MSC v.1916 64 bit (AMD64)]',
        'Authorization': 'Bearer '+token
    }

    try:
        CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        fh = urlopen(Request(url, headers=headers), context=CTX)
        with open(csv_path, "wb") as f:
            f.write(fh.read())
        f.close()
    except:
        print("fail to download CSV Will be re-download")

def get_file(filename: str, save_path: str,url:str,token:str):
    """
    # 下载文件函数
    :param filename: 文件名
    :param save_path: 保存文件路径
    :return: null
    """
    # URL修改位置
    url = url + filename
    # Token修改位置 Bearer
    headers = {
        'user-agent': 'tis/download.py_1.0--3.7.6rc1 (tags/v3.7.6rc1:bd18254b91, Dec 11 2019, 20:31:07) [MSC v.1916 64 bit (AMD64)]',
        'Authorization': 'Bearer '+token
    }

    try:
        CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        fh = urlopen(Request(url, headers=headers), context=CTX)
        with open(save_path, "wb") as f:
            f.write(fh.read())
        f.close()
    except:
        print("fail to download : ", filename,"Will be re-download")

def threading_download(filename_list: list, save_list: list,url:str,token:str):
    """
    # 多线程下载
    :param thread_num:线程数量
    :param filename_list:文件池
    :param save_list:保存池
    :param filesize_list:大小池
    :return:Null
    """
    threads = []
    u = len(filename_list)
    for i in range(u):
        t = threading.Thread(target=get_file, args=(filename_list[i], save_list[i],url,token))
        threads.append(t)
    for i in range(u):
        threads[i].start()
    for i in range(u):
        threads[i].join()
    return


def downmain(csv_path: str, save_dir: str, url:str,token:str, thread_num: int):
    """
    # 下载
    :param csv_path: csv路径
    :param save_dir: 文件下载目录
    :param log_path: Log文件保存地址
    :param thread_num: 线程数量
    :return: null
    """
    filename_list, save_list = read_csv(csv_path, save_dir)
    a = int(len(filename_list) / thread_num)
    for i in tqdm(range(a)):
        threading_download(filename_list[i * thread_num:(i + 1) * thread_num],save_list[i * thread_num:(i + 1) * thread_num],url,token)
    threading_download(filename_list[a * thread_num:len(filename_list)], save_list[a * thread_num:len(filename_list)],url,token)
    return


def log_check(csv_path: str, thread_num: int, save_dir: str,url:str,token:str) -> bool:
    """
    # 搜索失败的下载量
    :param csv_path: csv路径
    :param thread_num: 线程数量
    :param save_dir: 保存路径
    :return: Bool操作
    """
    filename_list, save_list = read_csv(csv_path, save_dir)
    files = os.listdir(save_dir)
    for file in files:
        if file in filename_list:
            filename_list.remove(file)
            save_list.remove(os.path.join(save_dir, file))
    if len(filename_list) == 0:
        return False
    else:
        a = int(len(filename_list) / thread_num)
        for i in tqdm(range(a)):
            threading_download(filename_list[i * thread_num:(i + 1) * thread_num],save_list[i * thread_num:(i + 1) * thread_num],url,token)
        threading_download(filename_list[a * thread_num:len(filename_list)],save_list[a * thread_num:len(filename_list)],url,token)
        return True

def main(url:str,save_dir:str,token:str,csv_path:str):
    """
    # 下载主函数
    :param url: URL
    :param save_dir: 保存路径
    :param token: TOKEN
    :param csv_path: csv路径
    :return:
    """
    thread_num = 20
    get_csv(csv_path,url,token)
    downmain(csv_path, save_dir, url,token, thread_num=thread_num)
    logflag = True
    while logflag:
        logflag = log_check(csv_path, thread_num, save_dir,url,token)

def _main(argv):
    """
    # 读取系统输入
    :param argv: argv
    :return: 执行函数main
    """
    parser = argparse.ArgumentParser(prog=argv[0], description="DOWNLOAD WITH THREADING")
    parser.add_argument('-u', '--url', dest='url', metavar='URL', help='LAAD DOWNLOAD URL', required=True)
    parser.add_argument('-s', '--saved', dest='saved', metavar='DIR',help='Store directory structure in DIR', required=True)
    parser.add_argument('-t', '--token', dest='token', metavar='TOK', help='Use app token TOK to authenticate',required=True)
    parser.add_argument('-c', '--csv', dest='csv', metavar='CSV', help='CSV PATH READING',required=True)
    args = parser.parse_args(argv[1:])
    if not os.path.exists(args.saved):
        os.makedirs(args.saved)
    return main(args.url,args.saved,args.token,args.csv)


if __name__ == '__main__':
    try:
        sys.exit(_main(sys.argv))
    except KeyboardInterrupt:
        sys.exit(-1)