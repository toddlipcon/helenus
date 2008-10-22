#!/usr/local/bin/thrift --java --php

#
# Interface definition for peer storage
# 

include "fb303.thrift"

namespace java  com.facebook.infrastructure.service


struct column_t {
   1: string                        columnName,
   2: string                        value,
   3: i64                           timestamp,
}

typedef map< string, list<column_t>  > column_family_map

struct batch_mutation_t {
   1: string                        table,
   2: string                        key,
   3: column_family_map             cfmap,
}

struct superColumn_t {
   1: string                        name,
   2: list<column_t>				columns,
}

typedef map< string, list<superColumn_t>  > superColumn_family_map

struct batch_mutation_super_t {
   1: string                        table,
   2: string                        key,
   3: superColumn_family_map        cfmap,
}


service Cassandra extends fb303.FacebookService {

  list<column_t> get_slice(string tablename,string key,string columnFamily_column, i32 start = -1 , i32 count = -1),
  column_t       get_column(string tablename,string key,string columnFamily_column),
  i32            get_column_count(string tablename,string key,string columnFamily_column),
  async void     insert(string tablename,string key,string columnFamily_column, string cellData,i64 timestamp),
  async void     batch_insert(batch_mutation_t batchMutation),
  bool           batch_insert_blocking(batch_mutation_t batchMutation),
  async void     remove(string tablename,string key,string columnFamily_column),
  list<column_t> get_columns_since(string tablename, string key, string columnFamily_column, i64 timeStamp),

  list<superColumn_t> get_slice_super(string tablename, string key, string columnFamily_superColumnName, i32 start = -1 , i32 count = -1),
  superColumn_t       get_superColumn(string tablename,string key,string columnFamily),
  async void          batch_insert_superColumn(batch_mutation_super_t batchMutationSuper),
  bool                batch_insert_superColumn_blocking(batch_mutation_super_t batchMutationSuper),
}

