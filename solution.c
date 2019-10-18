#include <stdbool.h>

#include "solution.h"

// TODO: improve has function to something better (search online).
int hash(int value, int size) { return value % size; }
// TODO: try other probing strategies, exponential back-off, rehashing.
int nextSlot(int currentSlot, int size) { return (currentSlot + 1) % size; }

struct HashTableSlot {
  bool isOccupied;
  int salesDate;
  int employee;
};

int Query1(struct Database* db, int managerID, int price) {
  // TODO: use orders as build side, since it has the condition
  // employeeManagerID == something and it turns out it has much
  // less tuples than the other.
  // TODO: implement partitioning.
  // TODO: change size of hash table.
  size_t size = db->itemsCardinality + 1;
  struct HashTableSlot hashTable[size];

  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < size; i++) {
    hashTable[i].isOccupied = false;
  }

  // Build hash table.
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    struct ItemTuple* buildInput = &db->items[i];
    if (buildInput->price >= price) {
      continue;
    }
    int hashValue = hash(buildInput->salesDate, size);
    while (hashTable[hashValue].isOccupied) {
      hashValue = nextSlot(hashValue, size);
    }
    hashTable[hashValue].isOccupied = true;
    hashTable[hashValue].salesDate = buildInput->salesDate;
    hashTable[hashValue].employee = buildInput->employee;
  }

  // Count matching tuples.
  int tuplesCount = 0;
  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* probeInput = &db->orders[i];
    if (probeInput->employeeManagerID != managerID) {
      continue;
    }
    int hashValue = hash(probeInput->salesDate, size);
    while (hashTable[hashValue].isOccupied) {
      // Keep on going even if we find a match because there could be
      // duplicates.
      if (hashTable[hashValue].salesDate == probeInput->salesDate &&
          hashTable[hashValue].employee == probeInput->employee) {
        tuplesCount++;
      }
      hashValue = nextSlot(hashValue, size);
    }
  }

  return tuplesCount;
}

int Query2(struct Database* db, int discount, int date) {
  (void)db;        // prevent compiler warning about unused variable
  (void)discount;  // prevent compiler warning about unused variable
  (void)date;      // prevent compiler warning about unused variable
  return 0;
}

int Query3(struct Database* db, int countryID) {
  (void)db;         // prevent compiler warning about unused variable
  (void)countryID;  // prevent compiler warning about unused variable
  return 0;
}

void CreateIndices(struct Database* db) {
  (void)db;  // prevent compiler warning about unused variable
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  db->indices = NULL;
}
