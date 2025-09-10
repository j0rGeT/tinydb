#include "tinydb.h"

static table_schema_t* find_table_schema(database_t *db, const char *table_name) {
    for (int i = 0; i < db->schema_count; i++) {
        if (strcmp(db->schemas[i].name, table_name) == 0) {
            return &db->schemas[i];
        }
    }
    return NULL;
}

int table_create(database_t *db, const char *table_name, column_def_t *columns, int column_count) {
    if (db->schema_count >= db->max_schemas) {
        return -1;
    }
    
    if (find_table_schema(db, table_name) != NULL) {
        return -1;
    }
    
    table_schema_t *schema = &db->schemas[db->schema_count];
    strncpy(schema->name, table_name, MAX_TABLE_NAME - 1);
    schema->name[MAX_TABLE_NAME - 1] = '\0';
    schema->column_count = column_count;
    
    for (int i = 0; i < column_count && i < MAX_COLUMNS; i++) {
        schema->columns[i] = columns[i];
    }
    
    page_t *root_page = storage_allocate_page(db);
    if (!root_page) {
        return -1;
    }
    
    // Initialize the entire page data to zero first
    memset(root_page->data, 0, PAGE_SIZE);
    
    btree_node_t *root_node = (btree_node_t*)root_page->data;
    root_node->is_leaf = 1;
    root_node->key_count = 0;
    
    pthread_mutex_lock(&root_page->page_mutex);
    root_page->is_dirty = 1;
    pthread_mutex_unlock(&root_page->page_mutex);
    
    // Save the root node to disk
    storage_write_page(db, root_page->page_id, root_page->data);
    
    schema->root_page_id = root_page->page_id;
    buffer_release_page(db->buffer_pool, root_page);
    
    db->schema_count++;
    return 0;
}

int table_drop(database_t *db, const char *table_name) {
    for (int i = 0; i < db->schema_count; i++) {
        if (strcmp(db->schemas[i].name, table_name) == 0) {
            for (int j = i; j < db->schema_count - 1; j++) {
                db->schemas[j] = db->schemas[j + 1];
            }
            db->schema_count--;
            return 0;
        }
    }
    return -1;
}

static value_t* extract_primary_key(table_schema_t *schema, tuple_t *tuple) {
    for (int i = 0; i < schema->column_count; i++) {
        if (schema->columns[i].is_primary_key) {
            return &tuple->values[i];
        }
    }
    return NULL;
}

static int store_tuple_in_page(database_t *db, page_id_t page_id, tuple_t *tuple, slot_id_t *slot) {
    page_t *page = buffer_get_page(db->buffer_pool, page_id);
    if (!page) return -1;
    
    char *page_data = page->data;
    int *tuple_count = (int*)page_data;
    
    if (*tuple_count >= (int)((PAGE_SIZE - sizeof(int)) / sizeof(tuple_t))) {
        buffer_release_page(db->buffer_pool, page);
        return -1;
    }
    
    tuple_t *tuples = (tuple_t*)(page_data + sizeof(int));
    tuples[*tuple_count] = *tuple;
    *slot = *tuple_count;
    (*tuple_count)++;
    
    pthread_mutex_lock(&page->page_mutex);
    page->is_dirty = 1;
    pthread_mutex_unlock(&page->page_mutex);
    
    storage_write_page(db, page_id, page->data);
    buffer_release_page(db->buffer_pool, page);
    
    return 0;
}

int tuple_insert(database_t *db, const char *table_name, tuple_t *tuple, transaction_id_t txn_id) {
    table_schema_t *schema = find_table_schema(db, table_name);
    if (!schema) return -1;
    
    if (tuple->column_count != schema->column_count) return -1;
    
    tuple->header.xmin = txn_id;
    tuple->header.xmax = 0;
    tuple->header.is_deleted = 0;
    
    page_t *data_page = storage_allocate_page(db);
    if (!data_page) return -1;
    
    slot_id_t slot;
    if (store_tuple_in_page(db, data_page->page_id, tuple, &slot) != 0) {
        buffer_release_page(db->buffer_pool, data_page);
        return -1;
    }
    
    value_t *primary_key = extract_primary_key(schema, tuple);
    if (primary_key) {
        if (btree_insert(db, schema->root_page_id, primary_key, 
                        data_page->page_id, slot) != 0) {
            buffer_release_page(db->buffer_pool, data_page);
            return -1;
        }
    }
    
    buffer_release_page(db->buffer_pool, data_page);
    return 0;
}

static tuple_t* load_tuple_from_page(database_t *db, page_id_t page_id, slot_id_t slot) {
    page_t *page = buffer_get_page(db->buffer_pool, page_id);
    if (!page) return NULL;
    
    // buffer_get_page already handles loading page data from disk if needed
    
    char *page_data = page->data;
    int *tuple_count = (int*)page_data;
    
    if ((int)slot >= *tuple_count) {
        buffer_release_page(db->buffer_pool, page);
        return NULL;
    }
    
    tuple_t *tuples = (tuple_t*)(page_data + sizeof(int));
    static tuple_t result_tuple;
    result_tuple = tuples[slot];
    
    buffer_release_page(db->buffer_pool, page);
    return &result_tuple;
}

int tuple_select(database_t *db, const char *table_name, value_t *key, 
                tuple_t **results, int *count, transaction_id_t txn_id) {
    table_schema_t *schema = find_table_schema(db, table_name);
    if (!schema) return -1;
    
    *count = 0;
    
    page_id_t tuple_page_id;
    slot_id_t tuple_slot;
    
    if (btree_search(db, schema->root_page_id, key, &tuple_page_id, &tuple_slot) == 0) {
        tuple_t *tuple = load_tuple_from_page(db, tuple_page_id, tuple_slot);
        if (tuple && mvcc_is_visible(&tuple->header, txn_id, db->txn_manager)) {
            *results = tuple;
            *count = 1;
            return 0;
        }
    }
    
    return 0;
}

int tuple_delete(database_t *db, const char *table_name, value_t *key, transaction_id_t txn_id) {
    table_schema_t *schema = find_table_schema(db, table_name);
    if (!schema) return -1;
    
    page_id_t tuple_page_id;
    slot_id_t tuple_slot;
    
    if (btree_search(db, schema->root_page_id, key, &tuple_page_id, &tuple_slot) == 0) {
        tuple_t *tuple = load_tuple_from_page(db, tuple_page_id, tuple_slot);
        if (tuple && mvcc_is_visible(&tuple->header, txn_id, db->txn_manager)) {
            mvcc_mark_deleted(&tuple->header, txn_id);
            
            page_t *page = buffer_get_page(db->buffer_pool, tuple_page_id);
            if (page) {
                char *page_data = page->data;
                tuple_t *tuples = (tuple_t*)(page_data + sizeof(int));
                tuples[tuple_slot] = *tuple;
                
                pthread_mutex_lock(&page->page_mutex);
                page->is_dirty = 1;
                pthread_mutex_unlock(&page->page_mutex);
                
                storage_write_page(db, tuple_page_id, page->data);
                buffer_release_page(db->buffer_pool, page);
            }
            return 0;
        }
    }
    
    return -1;
}