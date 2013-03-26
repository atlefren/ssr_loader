import simplejson
import time
import psycopg2
import config
from geojson.examples import SimpleWebFeature
from shapely.geometry import asShape
from geoalchemy2.types import Geography

#trim off excess spaces from strings
def trim_property(property):
    if isinstance(property, unicode):
        return property.strip()
    return property


#get the fields from the properties-dict, based on the table definition
def map_fields(properties, columns):
    ret = {}
    for column in columns:
        ret[column] = trim_property(properties[column])
    return ret

def createSimpleWebFeature(o):
    try:
        g = o['geometry']
        p = o['properties']
        return SimpleWebFeature(None,
                {'type': str(g.get('type')),
                 'coordinates': g.get('coordinates', [])},
            title=p.get('title'),
            summary=p.get('summary'),
            link=str(p.get('link')))
    except (KeyError, TypeError):
        pass
    return o

def create_insert(fields, table_name):
    query = "INSERT INTO " + table_name + " ("
    placeholders = "VALUES("
    values = ()
    for key, value in fields.iteritems():
        if key != "geog":
            query += str(key) + ", "
            placeholders += "%s, "
            values += (value,)
    query += "geog)"
    placeholders += "ST_GeogFromText(%s))"
    values += (fields["geog"],)
    return query + " " + placeholders, values

def map_ssr_fields(ssr_table):
    fields = []
    for column in ssr_table.columns:
        if not column.primary_key and not isinstance(column.type, Geography):
            fields.append(column.name)
    return fields

def ssr_2_postgis(file, ssr_table):

    table_name = ssr_table.name
    ssr_fields = map_ssr_fields(ssr_table)

    connection = psycopg2.connect("dbname=" + config.database["dbname"] + " user=" + config.database["user"])
    cur = connection.cursor()

    start_time = time.time()
    num_features_read = 0
    with open(file) as infile:
        for line in infile:
            try:
                feature =  simplejson.loads(line)
                swf = createSimpleWebFeature(feature)
                geom = asShape(swf.geometry)
                fields = map_fields(feature["properties"], ssr_fields)
                fields["geog"] = geom.wkt

                query, fields = create_insert(fields, table_name)
                try:
                    cur.execute(query, fields)
                    num_features_read+=1
                except psycopg2.IntegrityError, e:
                    print e

                if num_features_read%10000==0:
                    print "read %i features" % num_features_read
                    connection.commit()
            except simplejson.decoder.JSONDecodeError:
                pass
    connection.commit()
    cur.close()
    connection.close()
    elapsed_time = time.time() - start_time
    return num_features_read, elapsed_time