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

def q1_time(file_path):

    #Recibo Argumentos
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(sys.argv)
    pipeline_options = PipelineOptions(pipeline_args)

    #Ejecutamos pipeline
    return proceso1_1(pipeline_options, file_path)

def q2_time(file_path):

  # Creamos un contador vacío para almacenar los emojis y sus frecuencias
  emojis = collections.Counter()

  # Abrimos el archivo json con los datos de Twitter
  with open(file_path, "r") as f:
      for line in f:
          tweet = json.loads(line)
          text = tweet["content"]
          matches = em.emoji_list(text)
          if matches != []:
            emojis_list_ = [match["emoji"] for match in matches]
            emojis.update(emojis_list_)


  # Obtenemos el resultado ordenado por frecuencia
  result = emojis.most_common(10)
  print(result)

def q3_time(file_path):

  # Creamos un contador vacío para almacenar los emojis y sus frecuencias
  menciones = collections.Counter()

  # Abrimos el archivo json con los datos de Twitter
  with open(file_path, "r") as f:
      for line in f:
          tweet = json.loads(line)
          users_m = tweet["mentionedUsers"] #lista de menciones
          if users_m:
            menciones_list_ = [match["username"] for match in users_m]
            menciones.update(menciones_list_)


  # Obtenemos el resultado ordenado por frecuencia
  result = menciones.most_common()
  print(result)