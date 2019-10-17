#include "solution.h"
int hash(int value, int size){
  return value % size;
}
int nextSlot(int current_slot, int size){
  return (current_slot + 1) % size;
}

struct HashTableSlot {
  bool is_occupied;
  int sales_date;
  int employee;
};

int Query1(struct Database* db, int managerID, int price) {
  HashTableSlot* hash_table[db->itemsCardinality + 1];
  int touples_count = 0;
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    ItemTuple& build_input = build[i];
    int hash_value = hash(build_input.salesDate);
    while (hash_table[hash_value].is_occupied) {
      hash_value = nextSlot(hash_value);
    }
    hash_table[hash_value] = {true, build_input.sales_date, 
                              build_input.employee};
  }
  for (size_t i = 0; i < db->ordersCardinality; i++) {
    OrderTuple& probe_input = probe[i];
    int hash_value = hash(probe_input.salesDate);
    while (hash_table[hash_value].is_occupied && 
           hash_table[hash_value] != probe_input.salesDate) {
      hash_value = nextSlot(hash_value);
    }
    if (hash_table[hash_value].is_occupied) {
      touples_count++;
    }
  }
  return touples_count;
}

int Query2(struct Database* db, int discount, int date) {
  (void)db;       // prevent compiler warning about unused variable
  (void)discount; // prevent compiler warning about unused variable
  (void)date;     // prevent compiler warning about unused variable
  return 0;
}

int Query3(struct Database* db, int countryID) {
  (void)db;        // prevent compiler warning about unused variable
  (void)countryID; // prevent compiler warning about unused variable
  return 0;
}

void CreateIndices(struct Database* db) {
  (void)db; // prevent compiler warning about unused variable
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  db->indices = NULL;
}
