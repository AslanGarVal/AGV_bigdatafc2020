#!/usr/bin/env python
# coding: utf-8

# In[15]:


from hdfs import InsecureClient
import json


# In[59]:


def conexion(url, usuario):
    """
    url --string no null, url del host
    usuario --string no null, nombre del usuario
    
    Esta función logra la conexion con hdfs 
    
    """
    try:
        client = InsecureClient(url, user = usuario)
        return client
    except:
        print("Ocurrio un error favor de verificar la url del host")


# In[61]:


def crear_directorio(client,pathhdfs):
    """
    pathhdfs --string not null 
    Esta funcion crea un directorio en hdfs, pasandole la ruta donde sera creado
    """
    try:
        pathcreado=client.makedirs(pathhdfs)
        print("se creo el directorio " + pathhdfs)
        return pathcreado
    except:
        print("Ocurrio un error favor de verificar")


# In[30]:


def cargar_archivo(client,pathhdfs,local_path):
    """
    pathhdfs, local_path --string not null
    la función carga un archivo ubicado en local_path 
    en la ruta pathhdfs especificada 
    """
    try: #asegurarse de que el user puede escribir en el pathhdfs
        upload = client.upload(pathhdfs,local_path)
        print("se cargó el archivo " + local_path + " en " + pathhdfs)
        return upload
    except:
        print("Ocurrió un error, favor de verificar")


# In[31]:


def borrar_archivo(client, pathhdfs):
    """
    
    pathhdfs --string not null
    la función elimina el archivo ubicado en pathhdfs
    usando el cliente client
    """
    try:
        delete = client.delete(pathhdfs)
        print("se borró el archivo " + pathhdfs)
        return delete
    except:
        print("Algo salió mal, revisa de nuevo")


# In[42]:


def lista_directorio(client, pathhdfs):
    """
    
    pathhdfs --string not null
    la función lista los elementos del directorio ubicado en pathhdfs
    """
    try:
        contents = client.list(pathhdfs)
        print("El directorio " + pathhdfs + " contiene:")
        for element in contents:
            print(element)
    except:
        print("Algo salió mal, revisa de nuevo")


# In[67]:


def leer_archivo(client, pathhdfs):
    """
    
    pathhdfs --string not null
    lee el archivo ubicado en pathhdfs
    """
    try:
        with client.read(pathhdfs) as reader:
            content = reader.read()
        print(content)
    except:
        print('Algo salió mal, intente de nuevo')


# In[48]:


def eliminar_directorio(client, path_del_directorio):
    """
    path_del_directorio --string not null
    elimina el directorio y todos los archivos dentro
    """
    try:
        elim = client.delete(path_del_directorio, recursive = True)
        if elim:
            print("Se eliminó el directorio")
        else:
            print("No se eliminó")
    except:
        print('Algo salió mal')


# In[68]:


def main():
    test_file = {"nombre" : "fulano", "numero" : 554673297}
    with open("prueba.json","w") as output:
        json.dump(test_file,output)
    file='prueba.json'
    url='http://localhost:50070'
    path_hdfs = '/user/root/prueba'
    
    client = conexion(url, 'root')
    crear_directorio(client, path_hdfs)
    cargar_archivo(client, path_hdfs, file)
    lista_directorio(client, path_hdfs)
    leer_archivo(client, '/user/root/prueba/prueba.json')
    borrar_archivo(client, '/user/root/prueba/prueba.json')
    eliminar_directorio(client, path_hdfs)
    

if __name__=='__main__':
    main()
    

