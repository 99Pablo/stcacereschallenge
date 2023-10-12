import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse #
import sys
import datetime
import json
import collections
from typing import Tuple

import json
import collections
import heapq
import emoji as em


import json
import collections
import heapq
import emoji as em
# Define una función DoFn que devuelve el valor más repetido de la lista
class MostRepeatedFn(beam.DoFn):
  def process(self, element):
    key, values = element
    counter = collections.Counter(values)
    most_common_value = counter.most_common(1)[0]
    yield (key, most_common_value)

#Obtener fecha y usuario del tweet
def fecha_user(obj):
    if obj:
        fecha = str(obj.get("date"))[:10]
        user = obj.get("user",{}).get("username","")
        return (fecha,user)

# Generar elementos desde una lista
def generate_elements(elements):
    for element in elements:
      yield element

# Define una función que formatea los elementos de la lista según lo solicitado en el punto1
def formatter_1(elements):
      date_obj = datetime.datetime.strptime(elements[0], '%Y-%m-%d').date()
      return (date_obj,elements[2])


def f2(elemento):
      return elemento[0]

class MisOpciones(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--projectid', help = 'Proyecto ID')
     
def proceso1_1(pipeline_options,ruta):

  #creamos pipeline proceso
  with beam.Pipeline(options=pipeline_options) as p:

    #leer datos
    p_datos = ( p
              | "Leer Archivo" >> beam.io.ReadFromText(ruta)
              | "to Json"  >> beam.Map(json.loads)
              | "Obtener fecha-usuario" >> beam.Map(fecha_user).with_output_types(Tuple[str, str])
              | 'Groupby' >> beam.GroupByKey()
               )
    User_top1 = ( p_datos
              | "Conteo User" >> beam.ParDo(MostRepeatedFn())
              #| "print Salida1" >> beam.Map(print)
              )
    Top_dias = ( p_datos
              | "Conteo2" >> beam.MapTuple(lambda k, vs: (k, len(vs)))
              | "top 10" >> beam.combiners.Top.Of(10, key = lambda x: x[1])
              | "flat" >> beam.FlatMap(generate_elements)
             #| "print Salida" >> beam.Map(print)
    )
    joined = (({ 'p1': Top_dias, 'p2': User_top1 })
            | 'Join' >> beam.CoGroupByKey()
            | 'Filter' >> beam.Filter(lambda v: v[1]['p1'] != [])
            | "convertir a tupla" >> beam.Map (lambda x : (1,(x[0], x[1]['p1'][0],x[1]['p2'][0]))).with_output_types(Tuple[int,Tuple])
            | 'Groupby2' >> beam.GroupByKey()
            |  "Ordenar" >> beam.MapTuple(lambda k, vs: (k, sorted(vs,key=lambda x: x[1], reverse=True)))
            | "Formatear" >> beam.MapTuple(lambda k, vs: (k, list(map(formatter_1, vs))))
            | "output" >> beam.Values()
            | "print Salida" >> beam.Map(print)
            )

# Estraer Emojis
def get_emojis(text):
    matches = em.emoji_list(text)
    for match in matches:
        # Devolvemos solo el emoji usando yield
        yield match["emoji"]

# Estraer usuarios mencionados
def get_mencionados(matches):
    for match in matches:
        # Devolvemos solo el los usuarios mencionados usando yield
        yield match["username"]

def q1_memory(file_path):
  ar = open(file_path, "r", buffering=1024)

  # Crear un diccionario vacío para almacenar la frecuencia de los días
  fd = {}


  for linea in ar:
      tweet = json.loads(linea)
      dia = str(tweet.get("date"))[:10]
      fd[dia] = fd.get(dia, 0) + 1
  ar.close()
  del dia, ar, linea, tweet

  # Obtener el día con más tweets
  dias_maximo =tuple(map( f2 ,sorted(fd.items(), key=lambda x: -x[1])[:10]))
  del fd

  #creamos la lista de salida
  salida = []
  for d in dias_maximo:
      ar = open(file_path, "r", buffering=1024)
      # Crear un diccionario vacío para almacenar la frecuencia de los usuarios
      fu = {}
      for linea in ar:
          tweet = json.loads(linea)
          if str(tweet.get("date"))[:10] == d:
              usuario = tweet.get("user",{}).get("username","")
              fu[usuario] = fu.get(usuario, 0) + 1
      ar.close()
      del ar, linea, tweet, usuario
      salida.append((datetime.datetime.strptime(d, '%Y-%m-%d').date(),max(fu.items(), key=lambda x: x[1])[0]))
      del fu
  return salida


def q2_memory(file_path):
  #Aqui guardaremos las frecuencias
  emojis = {}

  # Abrimos el archivo json con los datos de Twitter
  with open(file_path, "r", buffering=1024) as f:
      for line in f:
          tweet = json.loads(line)
          text = tweet["content"]
          del tweet
          # Iteramos sobre los emojis del texto
          for emoji_ in get_emojis(text):
              emojis[emoji_] = emojis.get(emoji_, 0) + 1
          del text

  # Obtenemos la lista usando la función heapq.nlargest.
  result = heapq.nlargest(10, emojis.items(), key=lambda x: x[1])
  print(result)

def q3_memory(file_path):
  #Aqui guardaremos las frecuencias
  menciones = {}

  # Abrimos el archivo json con los datos de Twitter
  with open(file_path, "r", buffering=1024) as f:
      for line in f:
          tweet = json.loads(line)
          list_usu = tweet["mentionedUsers"]
          del tweet
          if list_usu:
            for usuario in get_mencionados(list_usu):
                menciones[usuario] = menciones.get(usuario, 0) + 1
          del list_usu

  # Obtenemos la lista usando la función heapq.nlargest.
  result = heapq.nlargest(len(menciones), menciones.items(), key=lambda x: x[1])
  print(result)
