/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"

enum avro_codec_type_t {
  AVRO_CODEC_NULL,
  AVRO_CODEC_DEFLATE,
  AVRO_CODEC_LZMA,
  AVRO_CODEC_SNAPPY
};
typedef enum avro_codec_type_t avro_codec_type_t;

struct avro_codec_t_ {
  const char *name;
  avro_codec_type_t type;
  int64_t block_size;
  int64_t used_size;
  void *block_data;
  void *codec_data;
};
typedef struct avro_codec_t_ *avro_codec_t;

#define check(rval, call)                                                      \
  {                                                                            \
    rval = call;                                                               \
    if (rval)                                                                  \
      return rval;                                                             \
  }

int avro_codec(avro_codec_t c, const char *type);
int avro_codec_reset(avro_codec_t c);
int avro_codec_encode(avro_codec_t c, void *data, int64_t len);
int avro_codec_decode(avro_codec_t c, void *data, int64_t len);

static int file_read_header(avro_reader_t reader, avro_schema_t *writers_schema,
                            avro_codec_t codec, char *sync, int synclen) {
  int rval;
  avro_schema_t meta_schema;
  avro_schema_t meta_values_schema;
  avro_value_iface_t *meta_iface;
  avro_value_t meta;
  char magic[4];
  avro_value_t codec_val;
  avro_value_t schema_bytes;
  const void *p;
  size_t len;

  check(rval, avro_read(reader, magic, sizeof(magic)));
  if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j' || magic[3] != 1) {
    avro_set_error("Incorrect Avro container file magic number");
    return EILSEQ;
  }

  meta_values_schema = avro_schema_bytes();
  meta_schema = avro_schema_map(meta_values_schema);
  meta_iface = avro_generic_class_from_schema(meta_schema);
  if (meta_iface == NULL) {
    return EILSEQ;
  }
  check(rval, avro_generic_value_new(meta_iface, &meta));
  rval = avro_value_read(reader, &meta);
  if (rval) {
    avro_prefix_error("Cannot read file header: ");
    return EILSEQ;
  }
  avro_schema_decref(meta_schema);

  rval = avro_value_get_by_name(&meta, "avro.codec", &codec_val, NULL);
  if (rval) {
    if (avro_codec(codec, NULL) != 0) {
      avro_set_error(
          "Codec not specified in header and unable to set 'null' codec");
      avro_value_decref(&meta);
      return EILSEQ;
    }
  } else {
    const void *buf;
    size_t size;
    char codec_name[11];

    avro_type_t type = avro_value_get_type(&codec_val);

    if (type != AVRO_BYTES) {
      avro_set_error("Value type of codec is unexpected");
      avro_value_decref(&meta);
      return EILSEQ;
    }

    avro_value_get_bytes(&codec_val, &buf, &size);
    memset(codec_name, 0, sizeof(codec_name));
    strncpy(codec_name, (const char *)buf, size < 10 ? size : 10);

    if (avro_codec(codec, codec_name) != 0) {
      avro_set_error("File header contains an unknown codec");
      avro_value_decref(&meta);
      return EILSEQ;
    }
  }

  rval = avro_value_get_by_name(&meta, "avro.schema", &schema_bytes, NULL);
  if (rval) {
    avro_set_error("File header doesn't contain a schema");
    avro_value_decref(&meta);
    return EILSEQ;
  }

  avro_value_get_bytes(&schema_bytes, &p, &len);
  rval = avro_schema_from_json_length((const char *)p, len, writers_schema);
  if (rval) {
    avro_prefix_error("Cannot parse file header: ");
    avro_value_decref(&meta);
    return rval;
  }

  avro_value_decref(&meta);
  avro_value_iface_decref(meta_iface);
  return avro_read(reader, sync, synclen);
}

struct avro_file_reader_t_ {
  avro_schema_t writers_schema;
  avro_reader_t reader;
  avro_reader_t block_reader;
  avro_codec_t codec;
  char sync[16];
  int64_t blocks_read;
  int64_t blocks_total;
  int64_t current_blocklen;
  char *current_blockdata;
};

#define check_prefix(rval, call, ...)                                          \
  {                                                                            \
    rval = call;                                                               \
    if (rval) {                                                                \
      avro_prefix_error(__VA_ARGS__);                                          \
      return rval;                                                             \
    }                                                                          \
  }

static int file_read_block_count(avro_file_reader_t r) {
  int rval;
  int64_t len;
  const avro_encoding_t *enc = &avro_binary_encoding;

  /* For a correctly formatted file, EOF will occur here */
  rval = enc->read_long(r->reader, &r->blocks_total);

  if (rval == EILSEQ && avro_reader_is_eof(r->reader)) {
    return EOF;
  }

  check_prefix(rval, rval, "Cannot read file block count: ");
  check_prefix(rval, enc->read_long(r->reader, &len),
               "Cannot read file block size: ");

  if (r->current_blockdata && len > r->current_blocklen) {
    r->current_blockdata =
        (char *)avro_realloc(r->current_blockdata, r->current_blocklen, len);
    r->current_blocklen = len;
  } else if (!r->current_blockdata) {
    r->current_blockdata = (char *)avro_malloc(len);
    r->current_blocklen = len;
  }

  if (len > 0) {
    check_prefix(rval, avro_read(r->reader, r->current_blockdata, len),
                 "Cannot read file block: ");

    check_prefix(rval, avro_codec_decode(r->codec, r->current_blockdata, len),
                 "Cannot decode file block: ");
  }

  avro_reader_memory_set_source(
      r->block_reader, (const char *)r->codec->block_data, r->codec->used_size);

  r->blocks_read = 0;
  return 0;
}

int avro_file_reader_fp(FILE *fp, const char *path, int should_close,
                        avro_file_reader_t *reader) {
  int rval;
  avro_file_reader_t r =
      (avro_file_reader_t)avro_new(struct avro_file_reader_t_);
  if (!r) {
    if (should_close) {
      fclose(fp);
    }
    avro_set_error("Cannot allocate file reader for %s", path);
    return ENOMEM;
  }

  r->reader = avro_reader_file_fp(fp, should_close);
  if (!r->reader) {
    if (should_close) {
      fclose(fp);
    }
    avro_set_error("Cannot allocate reader for file %s", path);
    avro_freet(struct avro_file_reader_t_, r);
    return ENOMEM;
  }
  r->block_reader = avro_reader_memory(0, 0);
  if (!r->block_reader) {
    avro_set_error("Cannot allocate block reader for file %s", path);
    avro_reader_free(r->reader);
    avro_freet(struct avro_file_reader_t_, r);
    return ENOMEM;
  }

  r->codec = (avro_codec_t)avro_new(struct avro_codec_t_);
  if (!r->codec) {
    avro_set_error("Could not allocate codec for file %s", path);
    avro_reader_free(r->reader);
    avro_freet(struct avro_file_reader_t_, r);
    return ENOMEM;
  }
  avro_codec(r->codec, NULL);

  rval = file_read_header(r->reader, &r->writers_schema, r->codec, r->sync,
                          sizeof(r->sync));
  if (rval) {
    avro_reader_free(r->reader);
    avro_codec_reset(r->codec);
    avro_freet(struct avro_codec_t_, r->codec);
    avro_freet(struct avro_file_reader_t_, r);
    return rval;
  }

  r->current_blockdata = NULL;
  r->current_blocklen = 0;

  rval = file_read_block_count(r);
  if (rval == EOF) {
    r->blocks_total = 0;
  } else if (rval) {
    avro_reader_free(r->reader);
    avro_codec_reset(r->codec);
    avro_freet(struct avro_codec_t_, r->codec);
    avro_freet(struct avro_file_reader_t_, r);
    return rval;
  }

  *reader = r;
  return 0;
}

/*-- PROCESSING A FILE --*/

static void process_file(const char *filename) {
  avro_file_reader_t reader;
  FILE *fp;
  int should_close;

  if (filename == NULL) {
    fp = stdin;
    filename = "<stdin>";
    should_close = 0;
  } else {
    fp = fopen(filename, "rb");
    should_close = 1;

    if (fp == NULL) {
      fprintf(stderr, "Error opening %s:\n  %s\n", filename, strerror(errno));
      exit(1);
    }
  }

  if (avro_file_reader_fp(fp, filename, 0, &reader)) {
    fprintf(stderr, "Error opening %s:\n  %s\n", filename, avro_strerror());
    if (should_close) {
      fclose(fp);
    }
    exit(1);
  }

  avro_schema_t wschema;
  avro_value_iface_t *iface;
  avro_value_t value;

  wschema = avro_file_reader_get_writer_schema(reader);
  iface = avro_generic_class_from_schema(wschema);
  avro_generic_value_new(iface, &value);

  int rval;

  while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
    char *json;

    if (avro_value_to_json(&value, 1, &json)) {
      fprintf(stderr, "Error converting value to JSON: %s\n", avro_strerror());
    } else {
      printf("%s\n", json);
      free(json);
    }

    avro_value_reset(&value);
  }

  // If it was not an EOF that caused it to fail,
  // print the error.
  if (rval != EOF) {
    fprintf(stderr, "Error: %s\n", avro_strerror());
  }

  avro_file_reader_close(reader);
  avro_value_decref(&value);
  avro_value_iface_decref(iface);
  avro_schema_decref(wschema);

  if (should_close) {
    fclose(fp);
  }
}

/*-- MAIN PROGRAM --*/

static void usage(void) {
  fprintf(stderr, "Usage: avrocat <avro data file>\n");
}

int main(int argc, char **argv) {
  char *data_filename;

  if (argc == 2) {
    data_filename = argv[1];
  } else {
    usage();
    exit(1);
  }

  /* Process the data file */
  process_file(data_filename);
  return 0;
}