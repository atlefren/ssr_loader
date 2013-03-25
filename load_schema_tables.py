import simplejson
import config
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from geoalchemy2.types import Geography

def load_json_file(file):
    f = open(file, "r")
    return simplejson.loads(f.read())

#setup the database
def setup_database():
    engine = create_engine('postgresql://' + config.database["user"] + '@localhost:5432/' + config.database["dbname"])
    metadata = MetaData()
    navntype = Table('navntype', metadata,
        Column('key', Integer,  primary_key=True, autoincrement=False),
        Column("name", String),
        Column("desc", String)
    )

    nty_gruppenr = Table('nty_gruppenr', metadata,
        Column('key', Integer,  primary_key=True, autoincrement=False),
        Column("name", String)
    )

    ssr = Table('ssr', metadata,
        Column('id', Integer,  primary_key=True),
        Column("skr_snskrstat", String),
        Column("enh_ssr_id", Integer),
        Column("for_kartid", String),
        Column("for_regdato", Integer),
        Column("skr_sndato", Integer),
        Column("enh_snmynd", String),
        Column("for_sist_endret_dt", Integer),
        Column("enh_snspraak", String),
        Column("nty_gruppenr", Integer, ForeignKey("nty_gruppenr.key")),
        Column("enh_snavn", String),
        Column("enh_komm", Integer),
        Column("enh_sntystat", String),
        Column("enh_navntype", Integer, ForeignKey("navntype.key")),
        Column("for_snavn", String),
        Column("kom_fylkesnr", Integer),
        Column("kpr_tekst", String),
        Column("geog", Geography(geometry_type='POINT', srid=4326)),
    )

    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine, navntype, nty_gruppenr, ssr

def load_data(engine, table, data):
    connection = engine.connect()
    for line in data:
        ins = table.insert(
            values=line
        )
        connection.execute(ins)
    connection.close()

def load_schema_data():
    navntype_data = load_json_file("navnetype.json")
    gruppe_data = load_json_file("nty_gruppenr.json")


    engine, navntype, nty_gruppenr, ssr_table = setup_database()

    load_data(engine, navntype, navntype_data)
    load_data(engine, nty_gruppenr, gruppe_data)

    return ssr_table