# -*- coding: utf-8 -*-
from wtpy.WtCoreDefs import WTSTickStruct
from ctypes import addressof,create_string_buffer,cast,pointer,POINTER
from socket import *
# 得将dtcfg.yaml里面的 broadcaster 下面的 active 改为true 本实例才可以接收到数据

def recv_from_datakit(host:str = '0.0.0.0', port:int = 9001):
    s = socket(AF_INET,SOCK_DGRAM)
    s.setsockopt(SOL_SOCKET,SO_BROADCAST,1)
    s.setsockopt(SOL_SOCKET,SO_REUSEADDR,1)
    s.bind((host, port))

    while True:
        try:

            msg,addr = s.recvfrom(1024)
            cstring = create_string_buffer(msg[4:])
            curTick = cast(pointer(cstring), POINTER(WTSTickStruct)).contents
            print("{}.{} - {} @ {}".format(curTick.exchg, curTick.code, curTick.price, curTick.action_time))

        except (KeyboardInterrupt,SyntaxError):
            raise


recv_from_datakit()