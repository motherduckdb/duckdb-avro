import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# null: no value
# boolean: a binary value
# int: 32-bit signed integer
# long: 64-bit signed integer
# float: single precision (32-bit) IEEE 754 floating-point number
# double: double precision (64-bit) IEEE 754 floating-point number
# bytes: sequence of 8-bit unsigned bytes
# string: unicode character sequence

# record
# detect recursive types or what happens here?

# {
#   "type": "record",
#   "name": "LongList",
#   "fields" : [
#     {"name": "value", "type": "long"},          
#     {"name": "next", "type": ["null", "LongList"]}
#   ]
# }

# enum

# {
#   "type": "enum",
#   "name": "Suit",
#   "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
# }

# array

# {
#   "type": "array",
#   "items" : "string",
#   "default": []
# }

# map

# {
#   "type": "map",
#   "values" : "long",
#   "default": {}
# }

# union

# ["null", "LongList"]

json_schema = """
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]}
 ]
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()







json_schema = """
{"namespace": "example2.avro",
 "type": "int",
 "name": "my_int"
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("root-int.avro", "wb"), DatumWriter(), schema)
writer.append(42)
writer.append(43)

writer.close()

reader = DataFileReader(open("root-int.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()



json_schema = """
{"namespace": "example3.avro",
 "type": ["int", "null"],
 "name": "my_union"
}
"""

schema = avro.schema.parse(json_schema)

writer = DataFileWriter(open("root-union.avro", "wb"), DatumWriter(), schema)
# writer.append(42)
# writer.append()

writer.close()

reader = DataFileReader(open("root-union.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()
