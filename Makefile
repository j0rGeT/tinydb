CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g -pthread
LDFLAGS = -pthread

SRCDIR = .
SOURCES = storage.c transaction.c btree.c table.c sql.c persistence.c
OBJECTS = $(SOURCES:.c=.o)

MAIN_SRC = main.c
TEST_SRC = test.c

TARGET = tinydb
TEST_TARGET = test_tinydb

.PHONY: all clean test

all: $(TARGET) $(TEST_TARGET)

$(TARGET): $(OBJECTS) $(MAIN_SRC:.c=.o)
	$(CC) $(LDFLAGS) -o $@ $^

$(TEST_TARGET): $(OBJECTS) $(TEST_SRC:.c=.o)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c tinydb.h
	$(CC) $(CFLAGS) -c $< -o $@

test: $(TEST_TARGET)
	./$(TEST_TARGET)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f *.o $(TARGET) $(TEST_TARGET) *.db

help:
	@echo "Available targets:"
	@echo "  all       - Build both main program and tests"
	@echo "  $(TARGET)   - Build the main TinyDB program"
	@echo "  $(TEST_TARGET) - Build the test program"
	@echo "  test      - Build and run tests"
	@echo "  run       - Build and run the main program"
	@echo "  clean     - Remove all built files and database files"
	@echo "  help      - Show this help message"

# Dependencies
storage.o: tinydb.h
transaction.o: tinydb.h
btree.o: tinydb.h
table.o: tinydb.h
sql.o: tinydb.h
persistence.o: tinydb.h
main.o: tinydb.h
test.o: tinydb.h