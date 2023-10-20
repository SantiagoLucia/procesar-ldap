import sqlalchemy
import pandas as pd
from config import CON_URL
from string import Template
from pathlib import Path
import re
import sys

sql = Template(
    """select
su.nombre_usuario,
du.apellido_nombre apellido_nombre,
decode(su.estado_registro, 0, 'BAJA', 1, 'ALTA') estado,
du.numero_cuit cuit,
du.mail,
c.cargo,
to_char(su.fecha_creacion,'DD/MM/YYYY') fecha_creacion,
decode(du.aceptacion_tyc, 0, 'NO', 1, 'SI') primer_ingreso,
ssi.codigo_sector_interno codigo_sector_interno,
ssi.nombre_sector_interno nombre_sector_interno,
sr.codigo_reparticion codigo_reparticion,
sr.nombre_reparticion  nombre_reparticion,
sr1.codigo_reparticion codigo_ministerio,
sr1.nombre_reparticion  nombre_ministerio
from 
track_ged.sade_sector_usuario su 
left join track_ged.sade_sector_interno ssi on (su.id_sector_interno = ssi.id_sector_interno) 
left join track_ged.sade_reparticion sr on (ssi.codigo_reparticion = sr.id_reparticion) 
left join track_ged.sade_reparticion sr1 on (sr.ministerio = sr1.id_reparticion)
left join co_ged.datos_usuario du on (du.usuario = su.nombre_usuario)
left join co_ged.cargos c on (du.cargo = c.id)  
where sr1.codigo_reparticion = '$codigo'
and su.estado_registro = 1
"""
)


def main(ldap_file: str, cod_org: str):
    # path del archivo ldap
    file_path = Path(ldap_file)

    # todos los usuarios del organismo cod_org
    engine = sqlalchemy.create_engine(CON_URL)
    with engine.connect() as conn:
        query = sqlalchemy.text(sql.substitute(codigo=cod_org))
        usuarios_organismo = pd.read_sql(query, conn)

    # expresion regular para obtener el USER
    pattern = re.compile("=(.*?),")

    # usuarios con permiso en el archivo ldap
    usuarios_permiso = [
        pattern.search(line).group(1)
        for line in open(file_path).readlines()
        if line.startswith("dn:")
    ]

    # me quedo solo con los usuarios que tengan permiso
    usuarios_org_permiso = usuarios_organismo[
        usuarios_organismo["nombre_usuario"].isin(usuarios_permiso)
    ]

    # guardo en el archivo de salida
    usuarios_org_permiso.to_csv(
        f"{file_path.stem}.csv",
        index=False,
        sep=";",
        mode="w",
        header=True,
        encoding="Windows-1252",
    )


if __name__ == "__main__":
    main(
        ldap_file=sys.argv[1],
        cod_org=sys.argv[2],
    )
