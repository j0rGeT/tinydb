#include "tinydb.h"
#include <unistd.h>

#define METADATA_PAGE_ID 1
#define SCHEMA_START_OFFSET sizeof(int)


int db_save_metadata(database_t *db) {
    metadata_t metadata;
    metadata.schema_count = db->schema_count;
    
    // Get the current next_page_id from the metadata page
    page_t *metadata_page = buffer_get_page(db->buffer_pool, METADATA_PAGE_ID);
    if (!metadata_page) return -1;
    
    metadata_t *current_metadata = (metadata_t*)metadata_page->data;
    metadata.next_page_id = current_metadata->next_page_id;
    buffer_release_page(db->buffer_pool, metadata_page);
    
    metadata_page = buffer_get_page(db->buffer_pool, METADATA_PAGE_ID);
    if (!metadata_page) return -1;
    
    
    // Only update the metadata portion, don't overwrite the entire page
    metadata_t *page_metadata = (metadata_t*)metadata_page->data;
    page_metadata->schema_count = metadata.schema_count;
    page_metadata->next_page_id = metadata.next_page_id;
    
    char *schema_data = metadata_page->data + sizeof(metadata_t);
    int remaining_space = PAGE_SIZE - sizeof(metadata_t);
    int schema_data_size = db->schema_count * sizeof(table_schema_t);
    
    if (schema_data_size > remaining_space) {
        printf("Schema data too large for metadata page!\n");
        buffer_release_page(db->buffer_pool, metadata_page);
        return -1;
    }
    
    memcpy(schema_data, db->schemas, schema_data_size);
    
    pthread_mutex_lock(&metadata_page->page_mutex);
    metadata_page->is_dirty = 1;
    pthread_mutex_unlock(&metadata_page->page_mutex);
    
    storage_write_page(db, METADATA_PAGE_ID, metadata_page->data);
    buffer_release_page(db->buffer_pool, metadata_page);
    
    return 0;
}

int db_load_metadata(database_t *db) {
    page_t *metadata_page = buffer_get_page(db->buffer_pool, METADATA_PAGE_ID);
    if (!metadata_page) {
        printf("Failed to get metadata page\n");
        return -1;
    }
    
    if (storage_read_page(db, METADATA_PAGE_ID, metadata_page->data) != 0) {
        printf("No existing metadata, initializing empty database\n");
        memset(metadata_page->data, 0, PAGE_SIZE);
        
        // Initialize metadata for new database
        metadata_t *metadata = (metadata_t*)metadata_page->data;
        metadata->schema_count = 0;
        metadata->next_page_id = 2; // Start from page 2 (page 1 is metadata)
        
        pthread_mutex_lock(&metadata_page->page_mutex);
        metadata_page->is_dirty = 1;
        pthread_mutex_unlock(&metadata_page->page_mutex);
        
        db->schema_count = 0;
        buffer_release_page(db->buffer_pool, metadata_page);
        return 0;
    }
    
    metadata_t *metadata = (metadata_t*)metadata_page->data;
    db->schema_count = metadata->schema_count;
    printf("Loaded metadata: schema_count=%d\n", db->schema_count);
    
    if (db->schema_count > 0) {
        char *schema_data = metadata_page->data + sizeof(metadata_t);
        int schema_data_size = db->schema_count * sizeof(table_schema_t);
        
        if ((size_t)schema_data_size <= PAGE_SIZE - sizeof(metadata_t)) {
            memcpy(db->schemas, schema_data, schema_data_size);
        } else {
            db->schema_count = 0;
            buffer_release_page(db->buffer_pool, metadata_page);
            return -1;
        }
    }
    
    buffer_release_page(db->buffer_pool, metadata_page);
    return 0;
}

int db_checkpoint(database_t *db) {
    if (db_save_metadata(db) != 0) {
        return -1;
    }
    
    pthread_mutex_lock(&db->buffer_pool->buffer_mutex);
    
    for (int i = 0; i < db->buffer_pool->capacity; i++) {
        page_t *page = &db->buffer_pool->pages[i];
        
        pthread_mutex_lock(&page->page_mutex);
        if (page->is_dirty && page->page_id != 0) {
            storage_write_page(db, page->page_id, page->data);
            page->is_dirty = 0;
        }
        pthread_mutex_unlock(&page->page_mutex);
    }
    
    pthread_mutex_unlock(&db->buffer_pool->buffer_mutex);
    
    if (db->data_file) {
        fflush(db->data_file);
        fsync(fileno(db->data_file));
    }
    
    return 0;
}

int db_recovery(database_t *db) {
    if (db_load_metadata(db) != 0) {
        printf("Failed to load database metadata\n");
        return -1;
    }
    
    printf("Database recovery completed. Found %d tables.\n", db->schema_count);
    
    for (int i = 0; i < db->schema_count; i++) {
        printf("Table: %s, Columns: %d, Root Page: %llu\n", 
               db->schemas[i].name, 
               db->schemas[i].column_count, 
               db->schemas[i].root_page_id);
    }
    
    return 0;
}

void db_periodic_checkpoint(database_t *db) {
    static time_t last_checkpoint = 0;
    time_t current_time = time(NULL);
    
    if (current_time - last_checkpoint >= 60) {
        printf("Performing periodic checkpoint...\n");
        if (db_checkpoint(db) == 0) {
            printf("Checkpoint completed successfully\n");
        } else {
            printf("Checkpoint failed\n");
        }
        last_checkpoint = current_time;
    }
}