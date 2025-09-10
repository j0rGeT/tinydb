#include "tinydb.h"

extern int sql_execute(database_t *db, const char *sql_string, transaction_id_t *current_txn);
extern int db_recovery(database_t *db);
extern int db_checkpoint(database_t *db);
extern void db_periodic_checkpoint(database_t *db);

void print_help() {
    printf("TinyDB - A simple relational database with MVCC support\n");
    printf("Commands:\n");
    printf("  CREATE TABLE table_name (col1 type, col2 type PRIMARY KEY, ...);\n");
    printf("  BEGIN;\n");
    printf("  INSERT INTO table_name VALUES (val1, val2, ...);\n");
    printf("  SELECT * FROM table_name [WHERE col = value];\n");
    printf("  DELETE FROM table_name WHERE col = value;\n");
    printf("  COMMIT;\n");
    printf("  ROLLBACK;\n");
    printf("  .help - Show this help\n");
    printf("  .checkpoint - Force checkpoint\n");
    printf("  .tables - List all tables\n");
    printf("  .exit - Exit the database\n");
    printf("\nSupported data types: INT, VARCHAR(size), FLOAT\n");
}

void list_tables(database_t *db) {
    printf("Tables in database:\n");
    if (db->schema_count == 0) {
        printf("  No tables found.\n");
        return;
    }
    
    for (int i = 0; i < db->schema_count; i++) {
        table_schema_t *schema = &db->schemas[i];
        printf("  %s (", schema->name);
        
        for (int j = 0; j < schema->column_count; j++) {
            if (j > 0) printf(", ");
            printf("%s ", schema->columns[j].name);
            
            switch (schema->columns[j].type) {
                case DATA_TYPE_INT:
                    printf("INT");
                    break;
                case DATA_TYPE_VARCHAR:
                    printf("VARCHAR(%d)", schema->columns[j].size);
                    break;
                case DATA_TYPE_FLOAT:
                    printf("FLOAT");
                    break;
            }
            
            if (schema->columns[j].is_primary_key) {
                printf(" PRIMARY KEY");
            }
        }
        printf(")\n");
    }
}

int main(int argc, char *argv[]) {
    const char *db_filename = "tinydb.db";
    
    if (argc > 1) {
        db_filename = argv[1];
    }
    
    printf("Starting TinyDB with file: %s\n", db_filename);
    
    database_t *db = db_create(db_filename);
    if (!db) {
        printf("Failed to create/open database\n");
        return 1;
    }
    
    printf("Database opened successfully\n");
    
    if (db_recovery(db) != 0) {
        printf("Database recovery failed\n");
        db_close(db);
        return 1;
    }
    
    transaction_id_t current_txn = 0;
    char input[1024];
    
    printf("TinyDB ready. Type .help for help or SQL commands.\n");
    
    while (1) {
        printf("tinydb> ");
        fflush(stdout);
        
        if (!fgets(input, sizeof(input), stdin)) {
            break;
        }
        
        char *trimmed = input;
        while (*trimmed == ' ' || *trimmed == '\t') trimmed++;
        
        char *end = trimmed + strlen(trimmed) - 1;
        while (end > trimmed && (*end == '\n' || *end == '\r' || *end == ' ')) {
            *end = '\0';
            end--;
        }
        
        if (strlen(trimmed) == 0) {
            continue;
        }
        
        if (strcmp(trimmed, ".help") == 0) {
            print_help();
            continue;
        }
        
        if (strcmp(trimmed, ".exit") == 0) {
            break;
        }
        
        if (strcmp(trimmed, ".checkpoint") == 0) {
            printf("Performing checkpoint...\n");
            if (db_checkpoint(db) == 0) {
                printf("Checkpoint completed successfully\n");
            } else {
                printf("Checkpoint failed\n");
            }
            continue;
        }
        
        if (strcmp(trimmed, ".tables") == 0) {
            list_tables(db);
            continue;
        }
        
        int result = sql_execute(db, trimmed, &current_txn);
        
        if (result == 0) {
            printf("OK\n");
        } else {
            printf("Error executing SQL statement\n");
        }
        
        db_periodic_checkpoint(db);
    }
    
    if (current_txn != 0) {
        printf("Auto-committing active transaction...\n");
        txn_commit(db, current_txn);
    }
    
    printf("Performing final checkpoint...\n");
    db_checkpoint(db);
    
    printf("Closing database...\n");
    db_close(db);
    
    printf("Goodbye!\n");
    return 0;
}