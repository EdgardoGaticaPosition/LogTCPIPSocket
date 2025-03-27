import socket
import time
import os
import sys
from os.path import exists
import threading

def cargar_variables_entorno():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../.env')
    load_dotenv(dotenv_path=env_path)
    logger.debug(f"Listener Server: {os.getenv('listener_server')}")
    logger.debug(f"Listener Port: {os.getenv('listener_port')}")
    logger.debug(f"Path log IMEI: {os.getenv('path_log_imei')}")
    logger.debug(f"Uvicacion de archivos log: {os.getenv('local_path_log')}")
    logger.debug(f"IMEI's: {os.getenv('imeis')}")
    logger.debug(f"Ano Mes: {os.getenv('anomes')}")

def threaded_process(thr,fichero,path,anomes,dia,port):
    """ Your main process which runs in thread for each chunk"""
    try:   
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error:
            print('Failed to create socket, threaded:'+str(thr))
            sys.exit()

        print('Socket Created : '+str(thr))

        ROBOT_IP = os.getenv('Listener_server')
        ROBOT_PORT = port
        client.connect((ROBOT_IP,ROBOT_PORT))

        print('Socket Connected to ' + ROBOT_IP + ', threaded:'+str(thr) )
        msg = client.recv(1024).decode('ascii')
        print(msg)                                                                         
        #api.my_operation(item)         
        print("Inicio threaded:"+ str(thr) +" ,archivo:" + fichero + "_" + anomes + dia + ".txt")
        archivos = path + fichero + "/" +  fichero + "_" + anomes + dia + ".txt"
        with open(archivos) as archivo:
            for linea in archivo:
                #2025-03-12 20:07:42_"
                print(linea + ', threaded:'+str(thr) )
                #Send cmd to Activate the robot
                cmd = linea[21:].replace('"', '')
                try:
                    client.send(bytes(cmd,'ascii'))
                    msg = client.recv(1024).decode('ascii')
                    #print(msg + ', threaded:'+str(thr))
            
                except socket.error:
                    print('Failed to send data, threaded:'+str(thr))  
        print("Fin threaded:"+ str(thr) +" ,archivo:" + fichero + "_" + anomes + dia + ".txt, threaded:")  
        client.close()                                                         
    except Exception as e:                                                       
        print('error with item' + str(e))  

def main():
    cargar_variables_entorno()
        # Splitting the items into chunks equal to number of threads

    arrPath=[{os.getenv('path_log_imei')}] 
    #"/home/egatica/Escritorio/TCP_GATEWAY/nPath/"
    local_path_log=os.getenv('local_path_log')
    IMEI=[{os.getenv('imeis')}] 
    dia=""
    p=0
    p_fin=len(arrPath)       
    anomes=os.getenv('anomes')

    for nPath in arrPath:
        for i in [12]:
            path=local_path_log+anomes+"xx/"         
            if i<=9:
                dia= "0" + str(i)
                sig_dia=""
                if i<31:
                    sig_dia="0" + str(i+1)
                else:
                    sig_dia="01"
            else:
                dia =  str(i)
                sig_dia2=""
                if i<31:
                    sig_dia2 = str(i+1)
                else:
                    sig_dia2="01"
            path=path.replace("xx",  dia)
            path=path.replace("nPath",  nPath)
            sDirectorio = path
            contenido = os.listdir(path)
            n_threads=len(contenido)
            thread_list = []             
            archivos_procesados = []
            thr=0
            for fichero in contenido:
                if fichero not in archivos_procesados:
                    if fichero in IMEI:
                        #File arch = new File(path +  archivo+ "/" + archivo + "_"+anomes+dia+".txt" );
                        if os.path.isfile(os.path.join(path + fichero , fichero + "_" + anomes + dia + ".txt" )):
                            if exists(path + fichero + "/" +  fichero + "_" + anomes + dia + ".txt"):
                                if thr<=n_threads:
                                    port=os.getenv('listener_port')                                        
                                    thread = threading.Thread(target=threaded_process, args=(thr,fichero,path,anomes,dia,port),)
                                    thread_list.append(thread)
                                    thread_list[thr].start()

                                    for thread in thread_list:
                                        thread.join()
                                    archivos_procesados.append(fichero)    
                                    thr=thr+1
    

# Run the main function
if __name__ == '__main__':
    try:
        # Paso inicial: Loguear el inicio de la aplicación

        main()
        # Loguear el éxito si todo se ejecuta correctamente

    except Exception as e:
        # Manejar errores críticos durante la ejecución
        print('Error crítico en la ejecución: :'+str(e)) 
        sys.exit(1)   