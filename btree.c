#include "tinydb.h"

static int value_compare(const value_t *a, const value_t *b) {
    if (a->type != b->type) return -2;
    
    switch (a->type) {
        case DATA_TYPE_INT:
            if (a->data.int_val < b->data.int_val) return -1;
            if (a->data.int_val > b->data.int_val) return 1;
            return 0;
        case DATA_TYPE_FLOAT:
            if (a->data.float_val < b->data.float_val) return -1;
            if (a->data.float_val > b->data.float_val) return 1;
            return 0;
        case DATA_TYPE_VARCHAR:
            return strcmp(a->data.str_val, b->data.str_val);
        default:
            return -2;
    }
}

btree_node_t* btree_create_node(database_t *db, int is_leaf) {
    page_t *page = storage_allocate_page(db);
    if (!page) return NULL;
    
    // Initialize the entire page data to zero first
    memset(page->data, 0, PAGE_SIZE);
    
    btree_node_t *node = (btree_node_t*)page->data;
    node->is_leaf = is_leaf;
    node->key_count = 0;
    
    // Initialize all pointers to 0
    if (is_leaf) {
        for (int i = 0; i < BTREE_ORDER - 1; i++) {
            node->pointers.leaf.tuple_page_ids[i] = 0;
            node->pointers.leaf.tuple_slots[i] = 0;
        }
    } else {
        for (int i = 0; i < BTREE_ORDER; i++) {
            node->pointers.children[i] = 0;
        }
    }
    
    pthread_mutex_lock(&page->page_mutex);
    page->is_dirty = 1;
    pthread_mutex_unlock(&page->page_mutex);
    
    return node;
}

static btree_node_t* btree_load_node(database_t *db, page_id_t page_id, page_t **page_handle) {
    printf("DEBUG: btree_load_node called for page %llu\n", page_id);
    page_t *page = buffer_get_page(db->buffer_pool, page_id);
    if (!page) {
        printf("btree_load_node: Failed to get page %llu\n", page_id);
        return NULL;
    }
    
    // buffer_get_page already handles loading page data from disk if needed
    // We trust that the page data in the buffer pool is correct
    
    // Debug: check what's actually in the loaded page data
    btree_node_t *node = (btree_node_t*)page->data;
    printf("btree_load_node: Successfully loaded page %llu, key_count: %d, is_leaf: %d, page_ptr: %p\n", 
           page_id, node->key_count, node->is_leaf, (void*)page);
    
    *page_handle = page;
    return node;
}

static void btree_save_node(database_t *db, page_id_t page_id, btree_node_t *node) {
    printf("btree_save_node: Saving page %llu, key_count: %d\n", page_id, node->key_count);
    
    // Debug: print keys being saved
    printf("btree_save_node: Node pointer: %p\n", (void*)node);
    for (int i = 0; i < node->key_count; i++) {
        printf("btree_save_node: Saving key[%d] = %d (addr: %p)\n", 
               i, node->keys[i].data.int_val, (void*)&node->keys[i]);
    }
    
    page_t *page = buffer_get_page(db->buffer_pool, page_id);
    if (!page) return;
    
    // Always copy data to ensure page data is updated
    printf("btree_save_node: Copying %zu bytes to page data at %p (node at %p)\n", 
           sizeof(btree_node_t), (void*)page->data, (void*)node);
    
    // Debug: check key values before copy
    btree_node_t *debug_node = (btree_node_t*)node;
    printf("btree_save_node: Before copy - key_count: %d\n", debug_node->key_count);
    for (int i = 0; i < debug_node->key_count; i++) {
        printf("btree_save_node: Before copy - key[%d] = %d at %p\n", 
               i, debug_node->keys[i].data.int_val, (void*)&debug_node->keys[i]);
    }
    
    memcpy(page->data, node, sizeof(btree_node_t));
    
    // Debug: check key values after copy
    btree_node_t *debug_page_node = (btree_node_t*)page->data;
    printf("btree_save_node: After copy - key_count: %d\n", debug_page_node->key_count);
    for (int i = 0; i < debug_page_node->key_count; i++) {
        printf("btree_save_node: After copy - key[%d] = %d at %p\n", 
               i, debug_page_node->keys[i].data.int_val, (void*)&debug_page_node->keys[i]);
    }
    
    pthread_mutex_lock(&page->page_mutex);
    page->is_dirty = 1;
    pthread_mutex_unlock(&page->page_mutex);
    
    buffer_release_page(db->buffer_pool, page);
    printf("btree_save_node: Page %llu marked as dirty\n", page_id);
}

static int btree_find_key_position(btree_node_t *node, const value_t *key) {
    int left = 0, right = node->key_count - 1;
    int pos = 0;
    
    while (left <= right) {
        int mid = (left + right) / 2;
        int cmp = value_compare(key, &node->keys[mid]);
        
        if (cmp == 0) {
            return mid;
        } else if (cmp < 0) {
            right = mid - 1;
            pos = mid;
        } else {
            left = mid + 1;
            pos = mid + 1;
        }
    }
    
    return pos;
}

static void btree_insert_key_at_position(btree_node_t *node, int pos, const value_t *key, 
                                        page_id_t tuple_page_id, slot_id_t tuple_slot) {
    printf("btree_insert_key_at_position: Inserting key at pos %d, current key_count: %d\n", pos, node->key_count);
    
    for (int i = node->key_count; i > pos; i--) {
        node->keys[i] = node->keys[i-1];
        if (node->is_leaf) {
            node->pointers.leaf.tuple_page_ids[i] = node->pointers.leaf.tuple_page_ids[i-1];
            node->pointers.leaf.tuple_slots[i] = node->pointers.leaf.tuple_slots[i-1];
        } else {
            node->pointers.children[i+1] = node->pointers.children[i];
        }
    }
    
    node->keys[pos] = *key;
    if (node->is_leaf) {
        node->pointers.leaf.tuple_page_ids[pos] = tuple_page_id;
        node->pointers.leaf.tuple_slots[pos] = tuple_slot;
        printf("btree_insert_key_at_position: Inserted key %d -> page %llu, slot %d\n", 
               key->data.int_val, tuple_page_id, tuple_slot);
    }
    node->key_count++;
    
    // Debug: verify key was actually stored
    printf("btree_insert_key_at_position: Verified key[%d] = %d at %p\n", 
           pos, node->keys[pos].data.int_val, (void*)&node->keys[pos]);
    printf("btree_insert_key_at_position: New key_count: %d\n", node->key_count);
}

static btree_node_t* btree_split_node(database_t *db, btree_node_t *full_node, 
                                     value_t *promoted_key, page_id_t *new_page_id) {
    btree_node_t *new_node = btree_create_node(db, full_node->is_leaf);
    if (!new_node) return NULL;
    
    int mid = BTREE_ORDER / 2;
    
    *promoted_key = full_node->keys[mid];
    
    new_node->key_count = full_node->key_count - mid - 1;
    for (int i = 0; i < new_node->key_count; i++) {
        new_node->keys[i] = full_node->keys[mid + 1 + i];
        if (full_node->is_leaf) {
            new_node->pointers.leaf.tuple_page_ids[i] = full_node->pointers.leaf.tuple_page_ids[mid + 1 + i];
            new_node->pointers.leaf.tuple_slots[i] = full_node->pointers.leaf.tuple_slots[mid + 1 + i];
        } else {
            new_node->pointers.children[i] = full_node->pointers.children[mid + 1 + i];
        }
    }
    
    if (!full_node->is_leaf) {
        new_node->pointers.children[new_node->key_count] = full_node->pointers.children[full_node->key_count];
    }
    
    full_node->key_count = mid;
    
    page_t *new_page = storage_allocate_page(db);
    if (new_page) {
        *new_page_id = new_page->page_id;
        buffer_release_page(db->buffer_pool, new_page);
    }
    
    return new_node;
}

static int btree_insert_recursive(database_t *db, page_id_t page_id, const value_t *key,
                                 page_id_t tuple_page_id, slot_id_t tuple_slot,
                                 value_t *promoted_key, page_id_t *new_page_id) {
    printf("btree_insert_recursive: Inserting into page %llu\n", page_id);
    page_t *page_handle = NULL;
    btree_node_t *node = btree_load_node(db, page_id, &page_handle);
    if (!node) return -1;
    
    if (node->is_leaf) {
        printf("btree_insert_recursive: Page %llu is leaf, key_count: %d\n", page_id, node->key_count);
        
        // Debug: print all keys before insertion
        printf("btree_insert_recursive: Page pointer: %p\n", (void*)node);
        for (int i = 0; i < node->key_count; i++) {
            printf("btree_insert_recursive: Before insert - Key[%d] = %d\n", i, node->keys[i].data.int_val);
        }
        
        int pos = btree_find_key_position(node, key);
        printf("btree_insert_recursive: Key position: %d, inserting key: %d\n", pos, key->data.int_val);
        
        if (pos < node->key_count && value_compare(key, &node->keys[pos]) == 0) {
            printf("btree_insert_recursive: Key already exists\n");
            btree_save_node(db, page_id, node);
            buffer_release_page(db->buffer_pool, page_handle);
            return -1;
        }
        
        if (node->key_count < BTREE_ORDER - 1) {
            printf("btree_insert_recursive: Node has space, inserting key\n");
            btree_insert_key_at_position(node, pos, key, tuple_page_id, tuple_slot);
            btree_save_node(db, page_id, node);
            buffer_release_page(db->buffer_pool, page_handle);
            return 0;
        } else {
            btree_insert_key_at_position(node, pos, key, tuple_page_id, tuple_slot);
            btree_node_t *new_node = btree_split_node(db, node, promoted_key, new_page_id);
            if (!new_node) {
                buffer_release_page(db->buffer_pool, page_handle);
                return -1;
            }
            
            btree_save_node(db, page_id, node);
            btree_save_node(db, *new_page_id, new_node);
            buffer_release_page(db->buffer_pool, page_handle);
            return 1;
        }
    } else {
        int pos = btree_find_key_position(node, key);
        page_id_t child_page_id = node->pointers.children[pos];
        
        value_t child_promoted_key;
        page_id_t child_new_page_id;
        
        int result = btree_insert_recursive(db, child_page_id, key, tuple_page_id, tuple_slot,
                                          &child_promoted_key, &child_new_page_id);
        
        if (result == 0) {
            btree_save_node(db, page_id, node);
            buffer_release_page(db->buffer_pool, page_handle);
            return 0;
        } else if (result == 1) {
            if (node->key_count < BTREE_ORDER - 1) {
                btree_insert_key_at_position(node, pos, &child_promoted_key, 0, 0);
                node->pointers.children[pos + 1] = child_new_page_id;
                btree_save_node(db, page_id, node);
                buffer_release_page(db->buffer_pool, page_handle);
                return 0;
            } else {
                btree_insert_key_at_position(node, pos, &child_promoted_key, 0, 0);
                node->pointers.children[pos + 1] = child_new_page_id;
                
                btree_node_t *new_node = btree_split_node(db, node, promoted_key, new_page_id);
                if (!new_node) {
                    buffer_release_page(db->buffer_pool, page_handle);
                    return -1;
                }
                
                btree_save_node(db, page_id, node);
                btree_save_node(db, *new_page_id, new_node);
                buffer_release_page(db->buffer_pool, page_handle);
                return 1;
            }
        }
        buffer_release_page(db->buffer_pool, page_handle);
    }
    
    return -1;
}

int btree_insert(database_t *db, page_id_t root_page_id, const value_t *key, 
                page_id_t tuple_page_id, slot_id_t tuple_slot) {
    value_t promoted_key;
    page_id_t new_page_id;
    
    int result = btree_insert_recursive(db, root_page_id, key, tuple_page_id, tuple_slot,
                                       &promoted_key, &new_page_id);
    
    if (result == 1) {
        btree_node_t *new_root = btree_create_node(db, 0);
        if (!new_root) return -1;
        
        new_root->key_count = 1;
        new_root->keys[0] = promoted_key;
        new_root->pointers.children[0] = root_page_id;
        new_root->pointers.children[1] = new_page_id;
        
        page_t *root_page = storage_allocate_page(db);
        if (root_page) {
            btree_save_node(db, root_page->page_id, new_root);
            buffer_release_page(db->buffer_pool, root_page);
        }
    }
    
    return (result >= 0) ? 0 : -1;
}

int btree_search(database_t *db, page_id_t root_page_id, const value_t *key,
                page_id_t *tuple_page_id, slot_id_t *tuple_slot) {
    page_id_t current_page_id = root_page_id;
    printf("btree_search: Starting search from root page %llu\n", root_page_id);
    
    while (current_page_id != 0) {
        printf("btree_search: Loading page %llu\n", current_page_id);
        page_t *page_handle = NULL;
        btree_node_t *node = btree_load_node(db, current_page_id, &page_handle);
        if (!node) {
            printf("btree_search: Failed to load page %llu\n", current_page_id);
            return -1;
        }
        
        if (node->is_leaf) {
            printf("btree_search: Page %llu is leaf, key_count: %d\n", current_page_id, node->key_count);
            
            // Debug: print all keys in this leaf node
            for (int i = 0; i < node->key_count; i++) {
                printf("btree_search: Key[%d] = %d\n", i, node->keys[i].data.int_val);
            }
            
            int pos = btree_find_key_position(node, key);
            printf("btree_search: Searching for key %d, found position: %d\n", key->data.int_val, pos);
            
            if (pos < node->key_count && value_compare(key, &node->keys[pos]) == 0) {
                *tuple_page_id = node->pointers.leaf.tuple_page_ids[pos];
                *tuple_slot = node->pointers.leaf.tuple_slots[pos];
                printf("btree_search: Found key at page %llu, slot %d\n", *tuple_page_id, *tuple_slot);
                buffer_release_page(db->buffer_pool, page_handle);
                return 0;
            }
            printf("btree_search: Key not found in leaf page %llu\n", current_page_id);
            buffer_release_page(db->buffer_pool, page_handle);
            return -1;
        } else {
            printf("btree_search: Page %llu is internal, key_count: %d\n", current_page_id, node->key_count);
            int pos = btree_find_key_position(node, key);
            page_id_t next_page_id = node->pointers.children[pos];
            buffer_release_page(db->buffer_pool, page_handle);
            current_page_id = next_page_id;
            printf("btree_search: Moving to child page %llu\n", current_page_id);
        }
    }
    
    printf("btree_search: Reached page 0, key not found\n");
    return -1;
}

int btree_delete(database_t *db, page_id_t root_page_id, const value_t *key) {
    (void)db; // Suppress unused parameter warning
    (void)root_page_id; // Suppress unused parameter warning
    (void)key; // Suppress unused parameter warning
    return 0;
}