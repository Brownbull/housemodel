# -*- coding: utf-8 -*-
"""
  @author: Brownbull - Gabriel Carcamo - carcamo.gabriel@gmail.com
  T4Houses - Transformation Module for houses data
"""
import pandas as pd
from include.files import *
from include.program import *

class House:
  def __init__(self, Srce, Province, PublishedDate, PropertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, Link):
    self.Srce = Srce
    self.Province = Province
    self.PublishedDate = PublishedDate
    self.PropertyType = PropertyType
    self.PropertyState = PropertyState
    self.MtTot = MtTot
    self.Bdroom = Bdroom
    self.Bath = Bath
    self.Parking = Parking
    self.PriceUF = PriceUF
    self.Link = Link

  def toCsvRow(self):
    return "{},{},{},{},{},{},{},{},{},{},{}\n".format(
      self.Srce ,
      self.Province ,
      self.PublishedDate ,
      self.PropertyType ,
      self.PropertyState ,
      self.MtTot ,
      self.Bdroom ,
      self.Bath ,
      self.Parking ,
      self.PriceUF,
      self.Link)

def etlTransform(outFile, snap, srce, idxRow, csvPath):
  # Initial Vars
  housesDf = pd.read_csv(csvPath)
  province = idxRow['Province']
  propertyType = idxRow['PropertyType']

  # get CSV
  with open(outFile, 'a') as outCsv:
    # WRITE DATA
    for idx, row in housesDf.iterrows():
      if srce == "portal inmobiliario":
        PublishedDate = "26-06-2020"
        PropertyState = "Propiedades usadas"
        MtTot = "150 m²" # rgxMt1
        Bdroom = "3"
        Bath = "2"
        Parking = "1"
        PriceUF = "get1(Price, PriceUnit)"
        Description ="\
        Se encuentra en un lugar privilegiado de Santiago centro, a paso de instituciones financieras, \
        Clinicas, centro comerciales, entre otros, con Inmejorable conectividad, cercano a Metro Línea 1 \
        (santa lucia), Línea 3 (Parque Almagro y Matta) y Línea 5 (Santa Isabel), amplio y con excelente \
        distribucion, muy bien mantenido y remodelado recientemente, piso fotolaminado en todo el departamento, \
        baños y pintura, Living comedor con ventanas en termopanel y  salida a la terraza, cocina independiente\
        equipada con encimera, horno empotrado y campana, conexión a gas y salida a logia, habitación principal\
        en suite con salida a una estupenda terraza de 72 M2, la cual cuenta con bodega, sirve para hacer \
        reuniones, asados en parrilla, también es funcional como patio para tender ropa, con orientación \
        Norte, oriente y sur, por lo que que se considera un plus en la propiedad, dos dormitorios adicionales\
        con baño común, el edificio cuenta con todas las comodidades, agua caliente central, vigilancia 24/7, \
        areas comunes como piscina e hidromasaje en azotea, quincho con vista panorámica , gimnasio \
        completamente equipado, sauna e hidromasaje, Gameroom con mesa de pool, business center, \
        home cinema, sala de juegos para niño, estacionamiento subterráneo para un vehiculo."
        houseToWrite = House( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, row['item-href'])
        outCsv.write(houseToWrite.toCsvRow())
      elif srce == "toctoc":
        PublishedDate = "26-06-2020"
        PropertyState = "Propiedades usadas"
        MtTot = "150 m²" # rgxMt1
        Bdroom = "3"
        Bath = "2"
        Parking = "1"
        PriceUF = "get2(FullPrice)"
        Description ="\
        Se encuentra en un lugar privilegiado de Santiago centro, a paso de instituciones financieras, \
        Clinicas, centro comerciales, entre otros, con Inmejorable conectividad, cercano a Metro Línea 1 \
        (santa lucia), Línea 3 (Parque Almagro y Matta) y Línea 5 (Santa Isabel), amplio y con excelente \
        distribucion, muy bien mantenido y remodelado recientemente, piso fotolaminado en todo el departamento, \
        baños y pintura, Living comedor con ventanas en termopanel y  salida a la terraza, cocina independiente\
        equipada con encimera, horno empotrado y campana, conexión a gas y salida a logia, habitación principal\
        en suite con salida a una estupenda terraza de 72 M2, la cual cuenta con bodega, sirve para hacer \
        reuniones, asados en parrilla, también es funcional como patio para tender ropa, con orientación \
        Norte, oriente y sur, por lo que que se considera un plus en la propiedad, dos dormitorios adicionales\
        con baño común, el edificio cuenta con todas las comodidades, agua caliente central, vigilancia 24/7, \
        areas comunes como piscina e hidromasaje en azotea, quincho con vista panorámica , gimnasio \
        completamente equipado, sauna e hidromasaje, Gameroom con mesa de pool, business center, \
        home cinema, sala de juegos para niño, estacionamiento subterráneo para un vehiculo."
        houseToWrite = House( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, row['item-href'])
        outCsv.write(houseToWrite.toCsvRow())
      elif srce == "propiedades emol":
        PublishedDate = "Fecha publicación:  \
														2020-07-07"
        PropertyState = "state1(Description)"
        MtTot = "M2 construidos: 110" # rgxMt2
        Bdroom = "Dormitorios: 3"
        Bath = "Baños: 2"
        Parking = "funcRgxPk"
        PriceUF = "get2(FullPrice)"
        Description = "Departamento remodelado en pleno centro de Santiago, de 110 metros cuadrados aproximados. Cercano a Metro Bellas Artes y Metro Santa Lucia, a pasos de Plaza de Armas, Municipalidad de Santiago, Mall Vivo Imperio. Apto para uso residencial o comercial.\
Cuenta con 3 dormitorios y 2 baños, ambos en suite. Living comedor con orientación Norte. Cocina tradicional. Pisos cerámicos en baños y cocina. Piso parquet en dormitorios, living y pasillos. Termo para agua caliente.\
No tiene estacionamiento ni bodega."
        houseToWrite = House( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, row['item-href'])
        outCsv.write(houseToWrite.toCsvRow())
      elif srce == "icasas":
        PublishedDate = snap
        PropertyState = "state1(Description)"
        MtTot = "120m2" # rgxMt3
        Bdroom = "3 Dormitorios" # rgxBd2
        Bath = "2 Baños" # rgxBt2
        Parking = "funcRgxPk"
        PriceUF = "get2(FullPrice)"
        Description = "Se vende amplio dpto 120 mts \
 ideal para oficinas, edificio histórico \
 san antonio 418, santiago \
 amplio departamento 120 mts, ideal para remodelar y acondicionar como oficina \
 3 dormitorios 2 baños, piso parquet \
 piso 6, con vista a la calle, año edificio 1952 \
 actualmente destino habitacional, factible para comercial \
 edificio de 10 pisos, departamentos, oficinas, laboratorios y locales comerciales en el primer piso. 3 ascensores \
 ubicación comercial, alta afluencia de público, locomoción \
 a 1 cuadra de plaza de armas y a 3del metro bellas artes, mall vivo imperio, teatro municipal, cerro santa lucía, notarias, entre otros. Edificio histórico (originalmente era la caja de empleados públicos y periodistas / caja de ee. Pp y pp), con una hermosa arquitectura y detalles en sus, paredes, escaleras y ascensores. Más información con soledad al \
 valor venta $168. 000. 000 conversable \
  comisión corredora de propiedades (2 iva) \
 gasto común $70. 000 \
 opción de arriendo $700. 000 mensual con gasto común incluido"
        houseToWrite = House( srce, province, snap, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, row['item-href'])
        outCsv.write(houseToWrite.toCsvRow())
      elif srce == "chile propiedades":
        PublishedDate = "31/05/2020"
        PropertyState = "state1(Description)"
        MtTot = "100 m²" # rgxMt1
        Bdroom = "3"
        Bath = "2"
        Parking = "1"
        PriceUF = "get2(FullPrice)"
        Description ="Ubicado en excelente sector de Ñuñoa, barrio residencial familiar, Exequiel Fernandez / Juan Gomez Millas, a pocas cuadras de METRO Chile España, cercano a comercios, Mall Portal Ñuñoa, Supermercado Jumbo y Lider, colegios de excelencia como San Gaspar, colegio Suizo y Divina Pastora, excelente conectividad.\
\
Departamento con terminaciones de primera calidad, año 2010 con buenas mantenciones, se encuentra en excelentes condiciones, orientación Nor-Oriente, en piso 13 con hermosa vista panorámica, superficie total 100mts2 aprox. Se distribuye en:\
- Cómodo hall de entrada \
- Amplio living comedor con salida a terraza en L, con aire acondicionado \
- Cocina amoblada y equipada con logia amplia \
- 3 dormitorios, todos de buen tamaño, con closet\
- 2 baños ambos con tina, uno de ellos en suite\
\
Terminaciones y equipamiento: Cuenta con aire acondicionado, alarma, muy buena distribución, piso laminado en dormitorios, living comedor y hall, Cocina con porcelanato.\
\
Incluye ESTACIONAMIENTO y BODEGA. \
Gastos comunes: $95.000 más consumo de agua caliente\
\
Espacios comunes: El edificio es de ambiente familiar tranquilo. Entre las áreas comunes destacan, gimnasio equipado, salas multiuso, quincho, piscina, lavandería y gran cantidad de estacionamientos de visita. \
\
Interesados ESCRIBIR mediante este portal o vía correo electrónico o whatsapp +56950198675 y responderemos sus consultas a la brevedad.\
Vende ACCIONA PROPIEDADE"
        houseToWrite = House( srce, province, PublishedDate, propertyType, PropertyState, MtTot, Bdroom, Bath, Parking,  PriceUF, row['item-href'])
        outCsv.write(houseToWrite.toCsvRow())