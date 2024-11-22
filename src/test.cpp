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