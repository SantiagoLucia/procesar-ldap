import sqlalchemy
import pandas as pd
from config import CON_URL
from string import Template
from pathlib import Path
import re
import sys
from glob import glob


sql = Template("""select
su.nombre_usuario,
du.apellido_nombre as apellido_nombre,
du.numero_cuit as cuit,
du.mail,
c.cargo,
sr.codigo_reparticion as codigo_reparticion,
sr.nombre_reparticion as nombre_reparticion,
'' as permiso_rce

from co_ged.datos_usuario du
inner join track_ged.sade_sector_usuario su 
   on du.usuario = su.nombre_usuario
inner join track_ged.sade_sector_interno ssi 
   on su.id_sector_interno = ssi.id_sector_interno
inner join track_ged.sade_reparticion sr 
   on ssi.codigo_reparticion = sr.id_reparticion
inner join track_ged.sade_reparticion sr1 
   on sr.ministerio = sr1.id_reparticion
inner join co_ged.cargos c 
    on du.cargo = c.id
inner join track_ged.sade_admin_reparticion rad
   on sr.id_reparticion = rad.id_reparticion
where rad.nombre_usuario = '$als'""")

export_path = "./export/*.txt"
salida_path = Path("salida")

# expresion regular para obtener el USER
pattern = re.compile("=(.*?),")


def main():
    
    # todos los usuarios que administra el als SROSER
    engine = sqlalchemy.create_engine(CON_URL)
    with engine.connect() as conn:
        query = sqlalchemy.text(sql.substitute(als="SROSER"))
        usuarios_als = pd.read_sql(query, conn)
    
    # usuarios con permiso en los archivos ldap
    usuarios_permiso = set()
    for file in glob(export_path):
        file_path = Path(file)
        print(f"Procesando {file_path.name}")
        for line in open(file_path).readlines():
            if line.startswith("dn:"):
                usuario = pattern.search(line).group(1)
                usuarios_permiso.add(usuario)
    
    # chequeo si tiene permiso rce y actualizo el dataframe
    for index, row in usuarios_als.iterrows():
        if row["nombre_usuario"] in usuarios_permiso:
            usuarios_als.at[index, "permiso_rce"] = "SI"  
        else:
            usuarios_als.at[index, "permiso_rce"] = "NO"
    
    # guardo en el archivo de salida
    usuarios_als.to_csv(
        salida_path / f"resultado.csv",
        index=False,
        sep=";",
        mode="w",
        header=True,
        encoding="Windows-1252",
        )

if __name__ == "__main__":
    main()