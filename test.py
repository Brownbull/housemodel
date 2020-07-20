def getProvince(rawProvince, rawProvincePref2, rawProvincePref3, default=-16):
  if rawProvince == "santiago":
    return "santiago"
  elif rawProvince == "huechuraba":
    return "huechuraba"
  elif rawProvince == "nunoa":
    return "nunoa"
  elif rawProvince == "providencia":
    return "providencia"
  elif rawProvince == "condes":
    return "las condes"
  elif rawProvince == "vitacura":
    return "vitacura"
  elif rawProvince == "barnechea":
    return "lo barnechea"
  elif rawProvince == "florida":
    return "la florida"
  elif rawProvince == "maipu":
    return "maipu"
  elif rawProvince == "penalolen":
    return "penalolen"
  elif rawProvince == "alto":
    return "puente alto"
  elif rawProvince == "central":
    return "estacion central"
  elif rawProvince == "normal":
    return "quinta normal"
  elif rawProvince == "prado":
    return "lo prado"
  elif rawProvince == "conchali":
    return "lo conchali"
  elif rawProvince == "quilicura":
    return "quilicura"
  elif rawProvince == "macul":
    return "macul"
  elif rawProvince == "reina":
    return "reina"
  elif rawProvince == "penaflor":
    return "penaflor"
  elif rawProvince == "colina":
    return "colina"
  elif rawProvince == "pirque":
    return "pirque"
  elif rawProvince == "independencia":
    return "independencia"
  elif rawProvince == "pudahuel":
    return "pudahuel"
  elif rawProvince == "recoleta":
    return "recoleta"
  elif rawProvince == "renca":
    return "renca"
  elif rawProvince == "lampa":
    return "lampa"
  elif rawProvince == "paine":
    return "paine"
  elif rawProvince == "tiltil":
    return "tiltil"
  elif rawProvince == "buin":
    return "buin"
  elif rawProvince == "melipilla":
    return "melipilla"
  elif rawProvince == "monte":
    return "el monte"
  elif rawProvince == "curacavi":
    return "curacavi"
  elif rawProvince == "cerrillos":
    return "cerrillos"
  elif rawProvince == "talagante":
    return "talagante"
  elif rawProvince == "granja":
    return "la granja"
  elif rawProvince == "pinto":
    return "maria pinto"
  elif rawProvince == "hurtado":
    return "padre hurtado"
  elif rawProvince == "cisterna":
    return "la cisterna"
  elif rawProvince == "miguel":
    return "san miguel"
  elif rawProvince == "bernardo":
    return "san bernardo"
  elif rawProvince == "bosque":
    return "el bosque"
  elif rawProvince == "pintana":
    return "la pintana"
  elif rawProvince == "joaquin":
    return "san joaquin"
  elif rawProvince == "ramon":
    return "san ramon"
  elif rawProvince == "navia":
    return "cerro navia"
  elif rawProvince == "cerda":
    return "pedro aguirre cerda"
  elif rawProvince == "tango":
    return "calera de tango"
  elif rawProvince == "maipo":
    if rawProvincePref3 == "jose":
      return "san jose de maipo"
    else:
      return "isla de maipo"
  else:
    return default

target = "inversionista"
if "inversion" in target:
  print("yes")
else:
  print("no")

