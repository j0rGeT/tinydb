#include "tinydb.h"
#include <ctype.h>

typedef enum {
    SQL_CREATE_TABLE,
    SQL_INSERT,
    SQL_SELECT,
    SQL_DELETE,
    SQL_BEGIN,
    SQL_COMMIT,
    SQL_ROLLBACK,
    SQL_UNKNOWN
} sql_command_t;

typedef struct {
    sql_command_t command;
    char table_name[MAX_TABLE_NAME];
    column_def_t columns[MAX_COLUMNS];
    int column_count;
    value_t values[MAX_COLUMNS];
    int value_count;
    value_t where_key;
    int has_where;
} sql_statement_t;

static void skip_whitespace(const char **sql) {
    while (**sql && isspace(**sql)) {
        (*sql)++;
    }
}

static int parse_identifier(const char **sql, char *buffer, int max_len) {
    skip_whitespace(sql);
    int i = 0;
    
    while (**sql && (isalnum(**sql) || **sql == '_') && i < max_len - 1) {
        buffer[i++] = **sql;
        (*sql)++;
    }
    buffer[i] = '\0';
    return i > 0;
}

static int parse_string(const char **sql, char *buffer, int max_len) {
    skip_whitespace(sql);
    
    if (**sql != '\'') return 0;
    (*sql)++;
    
    int i = 0;
    while (**sql && **sql != '\'' && i < max_len - 1) {
        buffer[i++] = **sql;
        (*sql)++;
    }
    
    if (**sql == '\'') {
        (*sql)++;
    }
    
    buffer[i] = '\0';
    return 1;
}

static int parse_integer(const char **sql, int *value) {
    skip_whitespace(sql);
    
    if (!isdigit(**sql) && **sql != '-') return 0;
    
    *value = 0;
    int sign = 1;
    
    if (**sql == '-') {
        sign = -1;
        (*sql)++;
    }
    
    while (**sql && isdigit(**sql)) {
        *value = *value * 10 + (**sql - '0');
        (*sql)++;
    }
    
    *value *= sign;
    return 1;
}

static int match_keyword(const char **sql, const char *keyword) {
    skip_whitespace(sql);
    const char *start = *sql;
    int len = strlen(keyword);
    
    if (strncasecmp(*sql, keyword, len) == 0) {
        *sql += len;
        if (!isalnum(**sql)) {
            return 1;
        }
    }
    
    *sql = start;
    return 0;
}

static int parse_create_table(const char **sql, sql_statement_t *stmt) {
    if (!match_keyword(sql, "TABLE")) return 0;
    
    if (!parse_identifier(sql, stmt->table_name, MAX_TABLE_NAME)) return 0;
    
    skip_whitespace(sql);
    if (**sql != '(') return 0;
    (*sql)++;
    
    stmt->column_count = 0;
    
    while (stmt->column_count < MAX_COLUMNS) {
        skip_whitespace(sql);
        if (**sql == ')') break;
        
        column_def_t *col = &stmt->columns[stmt->column_count];
        
        if (!parse_identifier(sql, col->name, MAX_COLUMN_NAME)) return 0;
        
        if (match_keyword(sql, "INT")) {
            col->type = DATA_TYPE_INT;
            col->size = sizeof(int);
        } else if (match_keyword(sql, "VARCHAR")) {
            col->type = DATA_TYPE_VARCHAR;
            col->size = MAX_VALUE_SIZE;
            skip_whitespace(sql);
            if (**sql == '(') {
                (*sql)++;
                int size;
                if (parse_integer(sql, &size)) {
                    col->size = size;
                }
                skip_whitespace(sql);
                if (**sql == ')') (*sql)++;
            }
        } else if (match_keyword(sql, "FLOAT")) {
            col->type = DATA_TYPE_FLOAT;
            col->size = sizeof(float);
        } else {
            return 0;
        }
        
        col->is_primary_key = 0;
        if (match_keyword(sql, "PRIMARY")) {
            if (match_keyword(sql, "KEY")) {
                col->is_primary_key = 1;
            }
        }
        
        stmt->column_count++;
        
        skip_whitespace(sql);
        if (**sql == ',') {
            (*sql)++;
        } else if (**sql == ')') {
            break;
        } else {
            return 0;
        }
    }
    
    skip_whitespace(sql);
    if (**sql != ')') return 0;
    (*sql)++;
    
    return 1;
}

static int parse_insert(const char **sql, sql_statement_t *stmt) {
    if (!match_keyword(sql, "INTO")) return 0;
    
    if (!parse_identifier(sql, stmt->table_name, MAX_TABLE_NAME)) return 0;
    
    if (!match_keyword(sql, "VALUES")) return 0;
    
    skip_whitespace(sql);
    if (**sql != '(') return 0;
    (*sql)++;
    
    stmt->value_count = 0;
    
    while (stmt->value_count < MAX_COLUMNS) {
        skip_whitespace(sql);
        if (**sql == ')') break;
        
        value_t *val = &stmt->values[stmt->value_count];
        
        if (**sql == '\'') {
            val->type = DATA_TYPE_VARCHAR;
            val->is_null = 0;
            if (!parse_string(sql, val->data.str_val, MAX_VALUE_SIZE)) return 0;
        } else if (isdigit(**sql) || **sql == '-') {
            const char *start = *sql;
            int int_val;
            if (parse_integer(sql, &int_val)) {
                val->type = DATA_TYPE_INT;
                val->is_null = 0;
                val->data.int_val = int_val;
            } else {
                *sql = start;
                return 0;
            }
        } else {
            return 0;
        }
        
        stmt->value_count++;
        
        skip_whitespace(sql);
        if (**sql == ',') {
            (*sql)++;
        } else if (**sql == ')') {
            break;
        } else {
            return 0;
        }
    }
    
    skip_whitespace(sql);
    if (**sql != ')') return 0;
    (*sql)++;
    
    return 1;
}

static int parse_where_clause(const char **sql, sql_statement_t *stmt) {
    if (!match_keyword(sql, "WHERE")) {
        stmt->has_where = 0;
        return 1;
    }
    
    stmt->has_where = 1;
    
    char column_name[MAX_COLUMN_NAME];
    if (!parse_identifier(sql, column_name, MAX_COLUMN_NAME)) return 0;
    
    skip_whitespace(sql);
    if (**sql != '=') return 0;
    (*sql)++;
    
    skip_whitespace(sql);
    if (**sql == '\'') {
        stmt->where_key.type = DATA_TYPE_VARCHAR;
        stmt->where_key.is_null = 0;
        if (!parse_string(sql, stmt->where_key.data.str_val, MAX_VALUE_SIZE)) return 0;
    } else if (isdigit(**sql) || **sql == '-') {
        int int_val;
        if (parse_integer(sql, &int_val)) {
            stmt->where_key.type = DATA_TYPE_INT;
            stmt->where_key.is_null = 0;
            stmt->where_key.data.int_val = int_val;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
    
    return 1;
}

static int parse_select(const char **sql, sql_statement_t *stmt) {
    if (!match_keyword(sql, "*")) return 0;
    
    if (!match_keyword(sql, "FROM")) return 0;
    
    if (!parse_identifier(sql, stmt->table_name, MAX_TABLE_NAME)) return 0;
    
    return parse_where_clause(sql, stmt);
}

static int parse_delete(const char **sql, sql_statement_t *stmt) {
    if (!match_keyword(sql, "FROM")) return 0;
    
    if (!parse_identifier(sql, stmt->table_name, MAX_TABLE_NAME)) return 0;
    
    return parse_where_clause(sql, stmt);
}

int sql_parse(const char *sql, sql_statement_t *stmt) {
    memset(stmt, 0, sizeof(sql_statement_t));
    
    const char *ptr = sql;
    
    if (match_keyword(&ptr, "CREATE")) {
        stmt->command = SQL_CREATE_TABLE;
        return parse_create_table(&ptr, stmt);
    } else if (match_keyword(&ptr, "INSERT")) {
        stmt->command = SQL_INSERT;
        return parse_insert(&ptr, stmt);
    } else if (match_keyword(&ptr, "SELECT")) {
        stmt->command = SQL_SELECT;
        return parse_select(&ptr, stmt);
    } else if (match_keyword(&ptr, "DELETE")) {
        stmt->command = SQL_DELETE;
        return parse_delete(&ptr, stmt);
    } else if (match_keyword(&ptr, "BEGIN")) {
        stmt->command = SQL_BEGIN;
        return 1;
    } else if (match_keyword(&ptr, "COMMIT")) {
        stmt->command = SQL_COMMIT;
        return 1;
    } else if (match_keyword(&ptr, "ROLLBACK")) {
        stmt->command = SQL_ROLLBACK;
        return 1;
    }
    
    stmt->command = SQL_UNKNOWN;
    return 0;
}

int sql_execute(database_t *db, const char *sql_string, transaction_id_t *current_txn) {
    sql_statement_t stmt;
    
    if (!sql_parse(sql_string, &stmt)) {
        printf("SQL parse error\n");
        return -1;
    }
    
    switch (stmt.command) {
        case SQL_CREATE_TABLE:
            return table_create(db, stmt.table_name, stmt.columns, stmt.column_count);
            
        case SQL_INSERT: {
            if (*current_txn == 0) {
                printf("No active transaction\n");
                return -1;
            }
            tuple_t tuple;
            tuple.column_count = stmt.value_count;
            for (int i = 0; i < stmt.value_count; i++) {
                tuple.values[i] = stmt.values[i];
            }
            return tuple_insert(db, stmt.table_name, &tuple, *current_txn);
        }
        
        case SQL_SELECT: {
            if (*current_txn == 0) {
                printf("No active transaction\n");
                return -1;
            }
            tuple_t *results;
            int count;
            value_t *key = stmt.has_where ? &stmt.where_key : NULL;
            int result = tuple_select(db, stmt.table_name, key, &results, &count, *current_txn);
            
            if (result == 0 && count > 0) {
                for (int i = 0; i < results->column_count; i++) {
                    switch (results->values[i].type) {
                        case DATA_TYPE_INT:
                            printf("%d\t", results->values[i].data.int_val);
                            break;
                        case DATA_TYPE_VARCHAR:
                            printf("%s\t", results->values[i].data.str_val);
                            break;
                        case DATA_TYPE_FLOAT:
                            printf("%.2f\t", results->values[i].data.float_val);
                            break;
                    }
                }
                printf("\n");
            }
            return result;
        }
        
        case SQL_DELETE: {
            if (*current_txn == 0) {
                printf("No active transaction\n");
                return -1;
            }
            if (!stmt.has_where) {
                printf("DELETE requires WHERE clause\n");
                return -1;
            }
            return tuple_delete(db, stmt.table_name, &stmt.where_key, *current_txn);
        }
        
        case SQL_BEGIN:
            *current_txn = txn_begin(db);
            return (*current_txn > 0) ? 0 : -1;
            
        case SQL_COMMIT:
            if (*current_txn == 0) {
                printf("No active transaction\n");
                return -1;
            }
            int commit_result = txn_commit(db, *current_txn);
            *current_txn = 0;
            return commit_result;
            
        case SQL_ROLLBACK:
            if (*current_txn == 0) {
                printf("No active transaction\n");
                return -1;
            }
            int rollback_result = txn_abort(db, *current_txn);
            *current_txn = 0;
            return rollback_result;
            
        default:
            printf("Unknown command\n");
            return -1;
    }
}