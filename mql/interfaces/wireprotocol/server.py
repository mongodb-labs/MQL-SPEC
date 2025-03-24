import socket
import sys
import os
import struct

from fpy.data.either import isLeft, isRight, fromLeft, fromRight

from mql.interfaces.wireprotocol.wireprotocol import parseMsg


def serve(port = 27017):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", port))
        sock.listen()
        print(f"listening on 127.0.0.1:{port}")
        conn, addr = sock.accept()
        with conn:
            print(f"connection from {addr}")
            while True:
                if not conn:
                    print("not conn, breaking")
                    break
                lenData = conn.recv(4)
                if not lenData:
                    break
                msgLen = struct.unpack("<i", lenData)[0]
                print(f"size: {msgLen}")
                rawMsg = list(lenData + conn.recv(msgLen))
                print(f"{rawMsg = }")
                msg = parseMsg(rawMsg)
                if isLeft(msg):
                    print("Failed parsing msg: ")
                    print(fromLeft(None, msg))
                    continue
                parsedMsg = fromRight(None, msg)
                assert(parsedMsg is not None)
                












if __name__ == "__main__":
    serve()
