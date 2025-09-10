#include "tinydb.h"

transaction_manager_t* txn_manager_create() {
    transaction_manager_t *manager = malloc(sizeof(transaction_manager_t));
    if (!manager) return NULL;
    
    manager->next_txn_id = 1;
    pthread_mutex_init(&manager->txn_manager_mutex, NULL);
    
    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
        manager->transactions[i].txn_id = 0;
        manager->transactions[i].state = TXN_STATE_ABORTED;
        pthread_mutex_init(&manager->transactions[i].txn_mutex, NULL);
    }
    
    return manager;
}

void txn_manager_destroy(transaction_manager_t *manager) {
    if (!manager) return;
    
    pthread_mutex_lock(&manager->txn_manager_mutex);
    
    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
        pthread_mutex_destroy(&manager->transactions[i].txn_mutex);
    }
    
    pthread_mutex_unlock(&manager->txn_manager_mutex);
    pthread_mutex_destroy(&manager->txn_manager_mutex);
    free(manager);
}

transaction_id_t txn_begin(database_t *db) {
    if (!db->txn_manager) {
        db->txn_manager = txn_manager_create();
        if (!db->txn_manager) return 0;
    }
    
    transaction_manager_t *manager = db->txn_manager;
    
    pthread_mutex_lock(&manager->txn_manager_mutex);
    
    int slot = -1;
    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
        if (manager->transactions[i].state == TXN_STATE_ABORTED || 
            manager->transactions[i].state == TXN_STATE_COMMITTED) {
            slot = i;
            break;
        }
    }
    
    if (slot == -1) {
        pthread_mutex_unlock(&manager->txn_manager_mutex);
        return 0;
    }
    
    transaction_id_t txn_id = manager->next_txn_id++;
    transaction_t *txn = &manager->transactions[slot];
    
    pthread_mutex_lock(&txn->txn_mutex);
    txn->txn_id = txn_id;
    txn->state = TXN_STATE_ACTIVE;
    txn->start_time = time(NULL);
    pthread_mutex_unlock(&txn->txn_mutex);
    
    pthread_mutex_unlock(&manager->txn_manager_mutex);
    
    return txn_id;
}

static transaction_t* find_transaction(transaction_manager_t *manager, transaction_id_t txn_id) {
    for (int i = 0; i < MAX_TRANSACTIONS; i++) {
        if (manager->transactions[i].txn_id == txn_id) {
            return &manager->transactions[i];
        }
    }
    return NULL;
}

int txn_commit(database_t *db, transaction_id_t txn_id) {
    if (!db->txn_manager) return -1;
    
    transaction_manager_t *manager = db->txn_manager;
    pthread_mutex_lock(&manager->txn_manager_mutex);
    
    transaction_t *txn = find_transaction(manager, txn_id);
    if (!txn) {
        pthread_mutex_unlock(&manager->txn_manager_mutex);
        return -1;
    }
    
    pthread_mutex_lock(&txn->txn_mutex);
    if (txn->state != TXN_STATE_ACTIVE) {
        pthread_mutex_unlock(&txn->txn_mutex);
        pthread_mutex_unlock(&manager->txn_manager_mutex);
        return -1;
    }
    
    txn->state = TXN_STATE_COMMITTED;
    pthread_mutex_unlock(&txn->txn_mutex);
    pthread_mutex_unlock(&manager->txn_manager_mutex);
    
    return 0;
}

int txn_abort(database_t *db, transaction_id_t txn_id) {
    if (!db->txn_manager) return -1;
    
    transaction_manager_t *manager = db->txn_manager;
    pthread_mutex_lock(&manager->txn_manager_mutex);
    
    transaction_t *txn = find_transaction(manager, txn_id);
    if (!txn) {
        pthread_mutex_unlock(&manager->txn_manager_mutex);
        return -1;
    }
    
    pthread_mutex_lock(&txn->txn_mutex);
    if (txn->state != TXN_STATE_ACTIVE) {
        pthread_mutex_unlock(&txn->txn_mutex);
        pthread_mutex_unlock(&manager->txn_manager_mutex);
        return -1;
    }
    
    txn->state = TXN_STATE_ABORTED;
    pthread_mutex_unlock(&txn->txn_mutex);
    pthread_mutex_unlock(&manager->txn_manager_mutex);
    
    return 0;
}

int mvcc_is_visible(tuple_header_t *header, transaction_id_t txn_id, transaction_manager_t *manager) {
    if (header->is_deleted) {
        return 0;
    }
    
    if (header->xmin > txn_id) {
        return 0;
    }
    
    pthread_mutex_lock(&manager->txn_manager_mutex);
    transaction_t *creating_txn = find_transaction(manager, header->xmin);
    if (creating_txn) {
        pthread_mutex_lock(&creating_txn->txn_mutex);
        if (creating_txn->state != TXN_STATE_COMMITTED) {
            if (creating_txn->txn_id != txn_id) {
                pthread_mutex_unlock(&creating_txn->txn_mutex);
                pthread_mutex_unlock(&manager->txn_manager_mutex);
                return 0;
            }
        }
        pthread_mutex_unlock(&creating_txn->txn_mutex);
    }
    
    if (header->xmax != 0 && header->xmax <= txn_id) {
        transaction_t *deleting_txn = find_transaction(manager, header->xmax);
        if (deleting_txn) {
            pthread_mutex_lock(&deleting_txn->txn_mutex);
            // If the deleting transaction is the current transaction, tuple is not visible
            if (deleting_txn->txn_id == txn_id) {
                pthread_mutex_unlock(&deleting_txn->txn_mutex);
                pthread_mutex_unlock(&manager->txn_manager_mutex);
                return 0;
            }
            // If the deleting transaction is committed, tuple is not visible
            if (deleting_txn->state == TXN_STATE_COMMITTED) {
                pthread_mutex_unlock(&deleting_txn->txn_mutex);
                pthread_mutex_unlock(&manager->txn_manager_mutex);
                return 0;
            }
            pthread_mutex_unlock(&deleting_txn->txn_mutex);
        } else {
            pthread_mutex_unlock(&manager->txn_manager_mutex);
            return 0;
        }
    }
    
    pthread_mutex_unlock(&manager->txn_manager_mutex);
    return 1;
}

void mvcc_mark_deleted(tuple_header_t *header, transaction_id_t txn_id) {
    header->xmax = txn_id;
}