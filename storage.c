#include "tinydb.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stddef.h>

#define METADATA_PAGE_ID 1

buffer_pool_t* buffer_pool_create(int capacity, database_t *db) {
    buffer_pool_t *pool = malloc(sizeof(buffer_pool_t));
    if (!pool) return NULL;
    
    pool->pages = calloc(capacity, sizeof(page_t));
    if (!pool->pages) {
        free(pool);
        return NULL;
    }
    
    pool->capacity = capacity;
    pool->count = 0;
    pool->db = db; // Store reference to database
    pthread_mutex_init(&pool->buffer_mutex, NULL);
    
    for (int i = 0; i < capacity; i++) {
        pthread_mutex_init(&pool->pages[i].page_mutex, NULL);
        pool->pages[i].page_id = 0;
        pool->pages[i].is_dirty = 0;
        pool->pages[i].pin_count = 0;
    }
    
    return pool;
}

void buffer_pool_destroy(buffer_pool_t *pool) {
    if (!pool) return;
    
    pthread_mutex_lock(&pool->buffer_mutex);
    
    for (int i = 0; i < pool->capacity; i++) {
        if (pool->pages[i].pin_count > 0) {
            printf("Warning: Page %llu still pinned during shutdown\n", pool->pages[i].page_id);
        }
        pthread_mutex_destroy(&pool->pages[i].page_mutex);
    }
    
    free(pool->pages);
    pthread_mutex_unlock(&pool->buffer_mutex);
    pthread_mutex_destroy(&pool->buffer_mutex);
    free(pool);
}

static int find_victim_page(buffer_pool_t *pool) {
    for (int i = 0; i < pool->capacity; i++) {
        if (pool->pages[i].pin_count == 0) {
            printf("find_victim_page: Choosing page %d (page_id %llu) as victim\n", i, pool->pages[i].page_id);
            return i;
        }
    }
    printf("find_victim_page: No victim page found!\n");
    return -1;
}

page_t* buffer_get_page(buffer_pool_t *pool, page_id_t page_id) {
    pthread_mutex_lock(&pool->buffer_mutex);
    
    for (int i = 0; i < pool->capacity; i++) {
        if (pool->pages[i].page_id == page_id && pool->pages[i].pin_count >= 0) {
            pthread_mutex_lock(&pool->pages[i].page_mutex);
            pool->pages[i].pin_count++;
            printf("buffer_get_page: Found page %llu at index %d, new pin_count: %d\n", 
                   page_id, i, pool->pages[i].pin_count);
            pthread_mutex_unlock(&pool->pages[i].page_mutex);
            pthread_mutex_unlock(&pool->buffer_mutex);
            return &pool->pages[i];
        }
    }
    
    printf("buffer_get_page: Page %llu not found in buffer pool, need to load\n", page_id);
    
    int victim_idx = find_victim_page(pool);
    if (victim_idx == -1) {
        pthread_mutex_unlock(&pool->buffer_mutex);
        return NULL;
    }
    
    // Debug: print buffer pool state
    printf("buffer_get_page: Buffer pool state - ");
    for (int i = 0; i < 5; i++) { // Only print first 5 slots for brevity
        if (pool->pages[i].page_id != 0) {
            printf("[%d]:%llu(pin:%d) ", i, pool->pages[i].page_id, pool->pages[i].pin_count);
        } else {
            printf("[%d]:empty ", i);
        }
    }
    printf("\n");
    
    page_t *victim_page = &pool->pages[victim_idx];
    pthread_mutex_lock(&victim_page->page_mutex);
    
    if (victim_page->is_dirty && victim_page->page_id != 0) {
        printf("buffer_get_page: Flushing dirty page %llu before eviction\n", victim_page->page_id);
        buffer_flush_page(pool, victim_page);
    }
    
    printf("buffer_get_page: Evicting page %llu from slot %d, loading page %llu\n", 
           victim_page->page_id, victim_idx, page_id);
    
    victim_page->page_id = page_id;
    victim_page->pin_count = 1;
    victim_page->is_dirty = 0;
    
    // Load page data from disk - use the database reference stored in buffer pool
    printf("buffer_get_page: About to call storage_read_page for page %llu\n", page_id);
    
    database_t *db = pool->db;
    printf("buffer_get_page: Using stored db pointer: %p, data_file: %p\n", (void*)db, (void*)(db ? db->data_file : NULL));
    
    if (storage_read_page(db, page_id, victim_page->data) != 0) {
        printf("buffer_get_page: Failed to load page %llu from disk\n", page_id);
        memset(victim_page->data, 0, PAGE_SIZE);
    }
    
    // Debug: check what was loaded for all pages
    btree_node_t *node = (btree_node_t*)victim_page->data;
    printf("buffer_get_page: Loaded page %llu, key_count: %d\n", page_id, node->key_count);
    for (int i = 0; i < node->key_count; i++) {
        printf("buffer_get_page: Loaded key[%d] = %d\n", i, node->keys[i].data.int_val);
    }
    
    printf("buffer_get_page: Loaded page %llu into buffer slot %d\n", page_id, victim_idx);
    
    pthread_mutex_unlock(&victim_page->page_mutex);
    pthread_mutex_unlock(&pool->buffer_mutex);
    
    return victim_page;
}

void buffer_release_page(buffer_pool_t *pool, page_t *page) {
    (void)pool; // Suppress unused parameter warning
    pthread_mutex_lock(&page->page_mutex);
    if (page->pin_count > 0) {
        page->pin_count--;
        printf("buffer_release_page: Released page %llu, new pin_count: %d\n", 
               page->page_id, page->pin_count);
    }
    pthread_mutex_unlock(&page->page_mutex);
}

void buffer_flush_page(buffer_pool_t *pool, page_t *page) {
    if (!page->is_dirty) return;
    
    printf("buffer_flush_page: Flushing page %llu\n", page->page_id);
    
    // Get the database reference from the pool
    database_t *db = pool->db;
    
    printf("buffer_flush_page: Using stored db pointer: %p, data_file: %p\n", (void*)db, (void*)(db ? db->data_file : NULL));
    
    if (db && db->data_file) {
        // Debug: check what's being flushed
        if (page->page_id == 1) {
            btree_node_t *node = (btree_node_t*)page->data;
            printf("buffer_flush_page: Flushing page %llu, key_count: %d\n", page->page_id, node->key_count);
            for (int i = 0; i < node->key_count; i++) {
                printf("buffer_flush_page: Key[%d] = %d\n", i, node->keys[i].data.int_val);
            }
        }
        storage_write_page(db, page->page_id, page->data);
    }
    
    page->is_dirty = 0;
}

static page_id_t allocate_new_page_id(database_t *db) {
    // Get the current next_page_id from metadata and increment it
    page_t *metadata_page = buffer_get_page(db->buffer_pool, METADATA_PAGE_ID);
    if (!metadata_page) return 0;
    
    metadata_t *metadata = (metadata_t*)metadata_page->data;
    page_id_t current_id = metadata->next_page_id;
    metadata->next_page_id++;
    
    pthread_mutex_lock(&metadata_page->page_mutex);
    metadata_page->is_dirty = 1;
    pthread_mutex_unlock(&metadata_page->page_mutex);
    
    buffer_release_page(db->buffer_pool, metadata_page);
    return current_id;
}

page_t* storage_allocate_page(database_t *db) {
    page_id_t new_page_id = allocate_new_page_id(db);
    page_t *page = buffer_get_page(db->buffer_pool, new_page_id);
    if (page) {
        pthread_mutex_lock(&page->page_mutex);
        page->is_dirty = 1;
        pthread_mutex_unlock(&page->page_mutex);
    }
    return page;
}

int storage_read_page(database_t *db, page_id_t page_id, char *buffer) {
    printf("storage_read_page: ENTRY - Reading page %llu\n", page_id);
    fflush(stdout);
    
    if (!db->data_file) {
        printf("storage_read_page: ERROR - No data file\n");
        fflush(stdout);
        return -1;
    }
    
    long position = (page_id - 1) * PAGE_SIZE;
    printf("storage_read_page: Reading page %llu at position %ld (file offset: %ld)\n", 
           page_id, position, ftell(db->data_file));
    
    // Debug: print call stack to see who's calling us
    printf("storage_read_page: CALL STACK - this should not happen for pages already in buffer!\n");
    
    fseek(db->data_file, position, SEEK_SET);
    size_t bytes_read = fread(buffer, 1, PAGE_SIZE, db->data_file);
    
    printf("storage_read_page: Attempted to read %d bytes from page %llu, actually read %zu bytes\n", 
           PAGE_SIZE, page_id, bytes_read);
    
    // Debug: check what was actually read for B-tree pages
    if (bytes_read > 0 && page_id != METADATA_PAGE_ID) {
        btree_node_t *node = (btree_node_t*)buffer;
        printf("storage_read_page: Read page %llu, is_leaf=%d, key_count: %d\n", 
               page_id, node->is_leaf, node->key_count);
        for (int i = 0; i < node->key_count; i++) {
            printf("storage_read_page: Key[%d] = %d\n", i, node->keys[i].data.int_val);
        }
    }
    
    return (bytes_read == PAGE_SIZE) ? 0 : -1;
}

int storage_write_page(database_t *db, page_id_t page_id, const char *buffer) {
    if (!db->data_file) return -1;
    
    long position = (page_id - 1) * PAGE_SIZE;
    printf("storage_write_page: Writing page %llu at position %ld (file offset: %ld)\n", 
           page_id, position, ftell(db->data_file));
    
    fseek(db->data_file, position, SEEK_SET);
    size_t bytes_written = fwrite(buffer, 1, PAGE_SIZE, db->data_file);
    fflush(db->data_file);
    
    // Debug: check what was actually written for B-tree pages (skip metadata page)
    if (page_id != METADATA_PAGE_ID) {
        btree_node_t *node = (btree_node_t*)buffer;
        printf("storage_write_page: Wrote page %llu, key_count: %d\n", page_id, node->key_count);
        for (int i = 0; i < node->key_count; i++) {
            printf("storage_write_page: Key[%d] = %d\n", i, node->keys[i].data.int_val);
        }
    }
    
    printf("storage_write_page: Wrote %zu bytes for page %llu\n", bytes_written, page_id);
    return (bytes_written == PAGE_SIZE) ? 0 : -1;
}

database_t* db_create(const char *filename) {
    printf("db_create: Creating database with file: %s\n", filename);
    
    database_t *db = malloc(sizeof(database_t));
    if (!db) return NULL;
    
    db->filename = strdup(filename);
    if (!db->filename) {
        free(db);
        return NULL;
    }
    
    db->data_file = fopen(filename, "r+b");
    if (!db->data_file) {
        printf("db_create: File doesn't exist, creating new file\n");
        db->data_file = fopen(filename, "w+b");
        if (!db->data_file) {
            printf("db_create: Failed to create file\n");
            free(db->filename);
            free(db);
            return NULL;
        }
    } else {
        printf("db_create: Opened existing file\n");
    }
    
    printf("db_create: File opened successfully, data_file pointer: %p\n", (void*)db->data_file);
    
    db->buffer_pool = buffer_pool_create(256, db);
    if (!db->buffer_pool) {
        fclose(db->data_file);
        free(db->filename);
        free(db);
        return NULL;
    }
    
    db->max_schemas = 9;
    db->schemas = malloc(sizeof(table_schema_t) * db->max_schemas);
    if (!db->schemas) {
        buffer_pool_destroy(db->buffer_pool);
        fclose(db->data_file);
        free(db->filename);
        free(db);
        return NULL;
    }
    
    db->schema_count = 0;
    
    printf("db_create: Database created successfully, db pointer: %p, data_file: %p\n", (void*)db, (void*)db->data_file);
    
    return db;
}

void db_close(database_t *db) {
    if (!db) return;
    
    buffer_pool_destroy(db->buffer_pool);
    
    if (db->data_file) {
        fclose(db->data_file);
    }
    
    free(db->schemas);
    free(db->filename);
    free(db);
}