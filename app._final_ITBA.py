import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from tkinter import messagebox
import requests
# defino el formate del tiempo al comenzar el codigo
format="%Y-%m-%d" 

##################### BLOQUE SOLICITAR DATOS A LA API #############################
def solicitarDatosTicker(ticker,fechaInicio,fechaFinal):#USAMOS LA API PARA SOLICITAR LA INFORMACION SOBRE EL TIKER
        fechaInicioStr=datetime.strftime(fechaInicio,format)
        fechaFinalStr=datetime.strftime(fechaFinal,format)
        tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{fechaInicioStr}/{fechaFinalStr}?apiKey=bp8d3cql6HSCj1R_0giun71zVCpuKoEz")
        tickerDataDict=tickerData.json()
        if tickerDataDict['status']=='OK' and tickerData.status_code == 200:
                        print("\tSolicitando Datos...")
                        return tickerDataDict['results']   #Esto es lo que devuelve                    
        else:
                print(f"Error al obtener datos de la API. Código de estado: {tickerData.status_code}")

def validarTicker():
    con=sqlite3.connect('TickerBaseDatos.db')
    df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
    con.close()
    listaTicker=list(df.Ticker)
    while True:
            try:
                ticker=input("Ingrese el ticker: ").upper()  
                listaTicker.index(ticker) 
            except ValueError:            
                tickerData=requests.get(f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-01/2023-01-10?apiKey=bp8d3cql6HSCj1R_0giun71zVCpuKoEz")
                tickerDataDict=tickerData.json()      
                if tickerDataDict['queryCount']!=0:
                    return listaTicker, ticker
                else: 
                    print("El ticker ingresado es incorrecto\n") 
                continue
            else:
                 break
    return listaTicker, ticker

######################## BLOQUE ARMAR BASE DE DATOS #############################
def crearBaseDatos():#ARMO UNA BASE DE DATOS EN SQLITE
    try:
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f''' CREATE TABLE IF NOT EXISTS TickerGuardados ( 
                                                Ticker TEXT PRIMARY KEY, FechaInicio TEXT, FechaFinal TEXT
                                                )''')
        conn.commit()
        conn.close()
    except sqlite3.OperationalError:
        return None

def tabla_ticker (datosTicker,nombreTabla):#ARMO UNA LISTA PARA CADA TIKER 
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()      
        c.execute (f'''CREATE TABLE {nombreTabla}(
                        Fecha TEXT, VolumenOperado REAL, PrecioPromedioPorVolumen REAL, PrecioApertura REAL, PrecioCierre REAL, 
                        PrecioMásAlto REAL, PrecioMásBajo REAL, NúmeroDeTransacciones REAL
                        )    ''')
        conn.commit()
        conn.close()
        insertar_ticker(datosTicker,nombreTabla) 
        print("\n LOS DATOS SE GUARDARON DE FORMA CORRECTA")      

def insertar_ticker(datosTicker,nombreTabla):#INGRAMOS TIKER EN CADA TABLA
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        fechaData = []
        fechaDataString=[]
        for i in range(0,len(datosTicker)):
            fechaData.append(datetime.fromtimestamp(datosTicker[i]['t']/1000.0).date())
            fechaDataString.append(fechaData[i].strftime(format))
            c.execute (f'''INSERT INTO {nombreTabla} (Fecha, VolumenOperado, PrecioPromedioPorVolumen, PrecioApertura, PrecioCierre, PrecioMásAlto,
                                                PrecioMásBajo, NúmeroDeTransacciones) 
                                VALUES ('{fechaDataString[i]}',{datosTicker[i]['v']},{datosTicker[i]['vw']},{datosTicker[i]['o']},
                                        {datosTicker[i]['c']},{datosTicker[i]['h']},{datosTicker[i]['l']},
                                        {datosTicker[i]['n']});
                                ''') 
        conn.commit()
        conn.close()
        ordenar_tiker(nombreTabla)  
        
def ordenar_tiker(nombreTabla):#ACOMODAMOS LOS TIKER EN FORMA ASCENDENTE
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f''' CREATE TABLE IF NOT EXISTS Datos_Ordenados AS SELECT * FROM {nombreTabla} ORDER BY Fecha ASC''')
        c.execute (f''' DROP TABLE IF EXISTS {nombreTabla}''')
        c.execute (f''' ALTER TABLE Datos_Ordenados RENAME TO {nombreTabla}''')
        conn.commit()
        conn.close()

def insertar_datos (ticker, fechaInicio, fechaFinal):#INSERTAMOS DATOS EN LA TABLA
         
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''INSERT INTO TickerGuardados (Ticker, FechaInicio, FechaFinal) 
                                       VALUES ('{ticker}','{fechaInicio}','{fechaFinal}');
                              ''')  
        conn.commit()
        conn.close()
        ordenar_datos()

def actualizar_datos(ticker, fechaInicio, fechaFinal):#ACTUALIZAMOS LOS DATOS GUARDADOS
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()
        c.execute (f'''UPDATE TickerGuardados
                        SET FechaInicio = '{fechaInicio}',
                            FechaFinal = '{fechaFinal}'
                        WHERE Ticker = '{ticker}'
                       ''')  
        conn.commit()
        conn.close()

def ordenar_datos():
         
        conn=sqlite3.connect(f"TickerBaseDatos.db")
        c = conn.cursor()        
        c.execute (f''' CREATE TABLE IF NOT EXISTS Ticker_Ordenados AS SELECT * FROM TickerGuardados ORDER BY Ticker''')
        c.execute (f''' DROP TABLE IF EXISTS TickerGuardados''')
        c.execute (f''' ALTER TABLE Ticker_Ordenados RENAME TO TickerGuardados''')
        conn.commit()
        conn.close()
#################### BLOQUE TRABAJAR DATOS #################
def actualizar_datos ():
        print("\nACTUALIZACIÓN DE DATOS\n")
        listaTicker=[]
        listaTicker, ticker= validarTicker()
        fechaInicio, fechaFinal = validar_fechas_ingresadas()
        verificar_dato(ticker, listaTicker,fechaInicio, fechaFinal)
def validar_fechas_ingresadas():# VALIDAMOS EL FORMATO DE LAS FECHAS INGRESADAS, Y SI ESTAN DE FORAM CORRECTA EL PROGRAMA CONTINUA 
        while True:
                while True:             #Valida Fecha Inicial
                        try:
                                #INGRESA LA FECHA EN FORMATO STRING
                                fechaInicioStr=input('Ingrese Fecha de Inicio (YYYY-MM-DD):')        
                                #CONVERTIMOS LA FECHA INGRESADA EN FORMATO DATETIME() PARA 
                                #PODER ANALIZARLA, Y LE DAMOS EL FORMATO DESEADO: format="%Y-%m-%d"
                                fechaInicio=datetime.strptime(fechaInicioStr,format).date() 
                                #Si el formato ingresado es incorrecto, nos devolverá un ValueError,
                                #nos mostrará el siguiente mensaje y deberá pedir que ingresemos la fecha otra vez.
                        except ValueError:
                                print("Ingrese la fecha en el formato correcto!")
                        else:
                                break
                while True:             #Valida Fecha Final con la misma lógica que la Fecha Inicial
                        try:
                                fechaFinalStr=input('Ingrese Fecha de Final (YYYY-MM-DD):')
                                fechaFinal=datetime.strptime(fechaFinalStr,format).date()      
                        except ValueError:
                                print("ingrese la fecha en el formato correcto")
                        else:
                                break
                
                #Acá, lo que hace es que compara la fechas ingresadas en formato Date(), y nos marca los errores
                if fechaInicio>fechaFinal:
                        print("ERROR! la fecha de inicio no puede ser mayor a la fecha final") 
                elif fechaInicio>datetime.now().date() or fechaFinal>datetime.now().date():
                        print(f"\n\tERROR!\n la fecha ingresada no puede ser mayor a la fecha actual {datetime.now().date()}\n")
                else: 
                        break
        #Si da todo bien, retorna las fechas ingresadas en formato date(). 
        return fechaInicio, fechaFinal         
def verificar_dato(ticker, listaTicker,fechaInicio, fechaFinal):
      
        try:    
                indiceTicker=listaTicker.index(ticker)
        except ValueError:  
                DatosTicker=solicitarDatosTicker(ticker,fechaInicio,fechaFinal)
                tabla_ticker(DatosTicker,ticker)
                insertar_datos(ticker, fechaInicio, fechaFinal)                
        else:                
                con=sqlite3.connect('TickerBaseDatos.db')
                df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
                con.close()
                listaTicker=list(df.Ticker)
                fechaIBD=datetime.strptime(df.FechaInicio[indiceTicker],format).date() #Fecha Inicial Base de Datos
                fechaFBD=datetime.strptime(df.FechaFinal[indiceTicker],format).date() #Fecha final Base Datos
                DeltaDia=timedelta(1)

                if fechaInicio<fechaIBD and fechaIBD<fechaFinal<fechaFBD:
                        fechaIS = fechaInicio
                        fechaFS = (fechaIBD-DeltaDia)
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        insertar_ticker(DatosTicker,ticker) #INSERTAR EN LA TABLA
                        actualizar_datos(ticker,fechaIS,fechaFBD)
                elif fechaIBD<fechaInicio<fechaFBD and fechaFBD<fechaFinal:
                        fechaIS = fechaFBD+DeltaDia
                        fechaFS = fechaFinal
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        insertar_ticker(DatosTicker,ticker) #INSERTAR EN LA TABLA
                        actualizar_datos(ticker,fechaIBD,fechaFinal)
                elif (fechaInicio<fechaIBD and fechaFinal>fechaFBD) or (fechaInicio<fechaIBD and fechaFinal<fechaFBD)or(fechaInicio>fechaIBD and fechaFinal>fechaFBD):
                        fechaIS=fechaInicio
                        fechaFS=fechaFinal
                        borrarTabla(ticker)
                        borrarRegistro(ticker)
                        DatosTicker=solicitarDatosTicker(ticker,fechaIS,fechaFS)
                        tabla_ticker(DatosTicker,ticker)
                        insertar_datos(ticker, fechaInicio, fechaFinal)   
                elif fechaInicio>fechaIBD and fechaFinal<fechaFBD:
                        print("\t tikers guardados")
################### BLOQUE MENU #######################
def menu_de_inicio():#EL MENU PRINCIPAL CONTIENE LAS OPCIONES QEU PUEDE ELEGIR EL USUARIO: ACTUALIZAR O VER DATOS
        while True: 
                print("\n <<<<< MENU DE INICIO >>>>>")
                print("\n 1. ACTUALIZACIÓN DE DATOS\n 2. VISUALIZACIÓN DE DATOS\n 3. SALIR")
                seleccion=input("\nINGRESE UNA OPCIÓN: ")   
                if seleccion == "1":
                        actualizar_datos()
                        
                elif seleccion == "2":
                        print("Eligio visualización de datos")
                        menu_ver_datos()
                        
                elif seleccion == "3":
                        print("Eligio la opción SALIR. HASTA LUEGO!!")
                        break
                else:
                        print("La opción seleccionada es incorrecta. Vuelva a intentarlo")

def menu_ver_datos():#DENTRO DEL MENU ARMO OTRO MENU PARA LAS OPCIONES DE VISUALIZACION DE DATOS
       while True: #SELECCIÓN DE MENÚ
            print("\n<<<<< VER  DATOS >>>>>:\n")
            print("\n 1. RESUMEN DE DATOS\n 2. GRÁFICO DE TICKER\n 3. VOLVER")#OPCIONES QEU PUEDE ELEGIR EL USUARIO
            seleccion=input("\nINGRESE LA OPCIÓN QUE DESEA: ")   
            if seleccion == "1":
                print("Eligio la opcion ... RESUMEN DE DATOS")
                visualizacionDatosAlmacenados()
                break
            elif seleccion == "2":
                print("Eligio la opcion ... GRAFICAR TICKER")
                graficarTicker()
                break
            elif seleccion == "3":
                print("Volviendo al menú de inicio")
                menu_de_inicio()
                break
            else:
                print("Opción incorrecta. Vuelva a intentarlo")

def visualizacionDatosAlmacenados():#EL MENU PERMITE VER LSO DATOS ALMACENADOS EN LA BASE DE DATOS 
        print("\nRESUMEN DE DATOS ALMACENADOS EN BASE DATOS")
        conn=sqlite3.connect('TickerBaseDatos.db')
        df=pd.read_sql(con=conn,sql="SELECT * FROM TickerGuardados")
        print("\nLos datos guardados en la base de datos son:\n")
        print(f'\tFecha Inicio<-\tTicker\t->\tFecha Final')
        for i in range (0,len(df.Ticker)):
                print(f'\t{df.FechaInicio[i]}\t<-\t{df.Ticker[i]}\t->\t{df.FechaFinal[i]}')
        conn.close()     

def graficarTicker():#EL MENU GRAFICO DE TIKER PERMITE EL USUARIO ELEGIR ENTRE VARIAS OPCOPNES LA VARIABLE QUE DESEA GRAFICAR
        con=sqlite3.connect('TickerBaseDatos.db')
        df=pd.read_sql(con=con,sql="SELECT * FROM TickerGuardados")
        con.close()
        listaTicker=list(df.Ticker)
        while True:
            try:
                tickerPrint=input("\nIngrese el nombre del tiker: ").upper()  
                listaTicker.index(tickerPrint)
                break  
            except ValueError: 
                print("\tEl ticker no esta en la base de datos, debe ir a ACTUALIZACIÓN DE DATOS.")
                continue
        con=sqlite3.connect('TickerBaseDatos.db')
        dPrint=pd.read_sql(con=con,sql=f"SELECT * FROM {tickerPrint}")
        con.close()
        while True:
                print("\n\tQUE DESEA GRAFICAR:")# AQUI SE PRESENTAN LAS OPCIONES DE VARIABLES A GRAFICAR 
                print("\n1.Volumen Operado\n2.Precio Apertura\n3.Precio Cierre\n4.Precio Más Alto\n5.Precio Más Bajo\n6.Número de Transacciones\n7. volver almenu de inicio ")
                seleccion = int(input(f"\nINGRESE LA OPCION QUE ELIGIO:"))
                if seleccion == 1:
                        print("Eligió Volumen Operado")
                        seleccionPrint = "VolumenOperado"
                elif seleccion == 2:
                        print("Eligió Precio Apertura")
                        seleccionPrint = "PrecioApertura"
                elif seleccion == 3:
                        print("Eligió Precio Cierre")
                        seleccionPrint = "PrecioCierre"
                elif seleccion == 4:
                        print("Eligió Precio Más Alto")
                        seleccionPrint = "PrecioMásAlto"
                elif seleccion == 5:
                        print("Eligió Precio Más Bajo")
                        seleccionPrint = "PrecioMásBajo"
                elif seleccion == 6:
                        print("Eligió Número de Transacciones")
                        seleccionPrint = "NúmeroDeTransacciones"
                elif seleccion == 7:
                        print ("volviendo al menu de inicio....")
                        menu_de_inicio()
                        break
                else:
                        print("Opción INCORRECTA. Vuelva a intentarlo")
                        # Puedes incluir un 'continue' aquí si es necesario

                dPrint.plot("Fecha", f"{seleccionPrint}", kind="line", title=f"{tickerPrint}")
                plt.show()

menu_de_inicio()
crearBaseDatos()             