#ifndef TINYDB_H
#define TINYDB_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#define PAGE_SIZE 4096
#define MAX_TABLE_NAME 64
#define MAX_COLUMN_NAME 32
#define MAX_COLUMNS 8
#define MAX_VALUE_SIZE 64
#define MAX_TRANSACTIONS 1024
#define BTREE_ORDER 49

typedef uint64_t transaction_id_t;
typedef uint64_t page_id_t;
typedef uint32_t slot_id_t;

typedef enum {
    DATA_TYPE_INT,
    DATA_TYPE_VARCHAR,
    DATA_TYPE_FLOAT
} data_type_t;

typedef enum {
    TXN_STATE_ACTIVE,
    TXN_STATE_COMMITTED,
    TXN_STATE_ABORTED
} transaction_state_t;

typedef struct {
    data_type_t type;
    char name[MAX_COLUMN_NAME];
    int size;
    int is_primary_key;
} column_def_t;

typedef struct {
    char name[MAX_TABLE_NAME];
    int column_count;
    column_def_t columns[MAX_COLUMNS];
    page_id_t root_page_id;
} table_schema_t;

typedef struct {
    int schema_count;
    int next_page_id;
    char reserved[PAGE_SIZE - 2 * sizeof(int) - 9 * sizeof(table_schema_t)];
} metadata_t;

typedef struct {
    union {
        int int_val;
        float float_val;
        char str_val[MAX_VALUE_SIZE];
    } data;
    data_type_t type;
    int is_null;
} value_t;

typedef struct {
    transaction_id_t xmin;
    transaction_id_t xmax;
    int is_deleted;
} tuple_header_t;

typedef struct {
    tuple_header_t header;
    value_t values[MAX_COLUMNS];
    int column_count;
} tuple_t;

typedef struct {
    page_id_t page_id;
    char data[PAGE_SIZE];
    int is_dirty;
    int pin_count;
    pthread_mutex_t page_mutex;
} page_t;

// Forward declaration for database_t
struct database_s;

typedef struct {
    page_t *pages;
    int capacity;
    int count;
    pthread_mutex_t buffer_mutex;
    struct database_s *db; // Reference to the database that owns this buffer pool
} buffer_pool_t;

typedef struct {
    transaction_id_t txn_id;
    transaction_state_t state;
    time_t start_time;
    pthread_mutex_t txn_mutex;
} transaction_t;

typedef struct {
    transaction_t transactions[MAX_TRANSACTIONS];
    transaction_id_t next_txn_id;
    pthread_mutex_t txn_manager_mutex;
} transaction_manager_t;

typedef struct {
    int is_leaf;
    int key_count;
    value_t keys[BTREE_ORDER - 1];
    union {
        page_id_t children[BTREE_ORDER];
        struct {
            page_id_t tuple_page_ids[BTREE_ORDER - 1];
            slot_id_t tuple_slots[BTREE_ORDER - 1];
        } leaf;
    } pointers;
} btree_node_t;

typedef struct database_s {
    FILE *data_file;
    char *filename;
    buffer_pool_t *buffer_pool;
    transaction_manager_t *txn_manager;
    table_schema_t *schemas;
    int schema_count;
    int max_schemas;
} database_t;

database_t* db_create(const char *filename);
void db_close(database_t *db);
int db_load_metadata(database_t *db);
int db_save_metadata(database_t *db);

int table_create(database_t *db, const char *table_name, column_def_t *columns, int column_count);
int table_drop(database_t *db, const char *table_name);

transaction_id_t txn_begin(database_t *db);
int txn_commit(database_t *db, transaction_id_t txn_id);
int txn_abort(database_t *db, transaction_id_t txn_id);

int mvcc_is_visible(tuple_header_t *header, transaction_id_t txn_id, transaction_manager_t *manager);
void mvcc_mark_deleted(tuple_header_t *header, transaction_id_t txn_id);

int tuple_insert(database_t *db, const char *table_name, tuple_t *tuple, transaction_id_t txn_id);
int tuple_delete(database_t *db, const char *table_name, value_t *key, transaction_id_t txn_id);
int tuple_select(database_t *db, const char *table_name, value_t *key, tuple_t **results, int *count, transaction_id_t txn_id);

buffer_pool_t* buffer_pool_create(int capacity, database_t *db);
void buffer_pool_destroy(buffer_pool_t *pool);
page_t* buffer_get_page(buffer_pool_t *pool, page_id_t page_id);
void buffer_release_page(buffer_pool_t *pool, page_t *page);
void buffer_flush_page(buffer_pool_t *pool, page_t *page);

int btree_insert(database_t *db, page_id_t root_page_id, const value_t *key, page_id_t tuple_page_id, slot_id_t tuple_slot);
int btree_search(database_t *db, page_id_t root_page_id, const value_t *key, page_id_t *tuple_page_id, slot_id_t *tuple_slot);
int btree_delete(database_t *db, page_id_t root_page_id, const value_t *key);

page_t* storage_allocate_page(database_t *db);
int storage_read_page(database_t *db, page_id_t page_id, char *buffer);
int storage_write_page(database_t *db, page_id_t page_id, const char *buffer);
btree_node_t* btree_create_node(database_t *db, int is_leaf);

int db_recovery(database_t *db);
int db_checkpoint(database_t *db);

#endif