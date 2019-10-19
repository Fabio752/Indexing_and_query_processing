#include <stdbool.h>

#include "solution.h"

// DEBUG
#include <stdio.h>

// TODO: improve has function to something better (search online).
int hash(int value, int size) { return value % size; }

int nextSlotLinear(int currentSlot, int size) { return (currentSlot + 1) % size; }
int nextSlotExpo(int currentSlot, int size, int backOff){ return (currentSlot + backOff) % size; }
int nextSlotRehashed(int currentSlot, int size){ return ((currentSlot + 31) * 17) % size; }

struct HashTableSlot {
  bool isOccupied;
  int salesDate;
  int employee;
};

int Query1(struct Database* db, int managerID, int price) {
  // TODO: implement partitioning.
  // TODO: change size of hash table.
  size_t size = db->ordersCardinality + 1;
  struct HashTableSlot hashTable[size];

  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < size; i++) {
    hashTable[i].isOccupied = false;
  }

  int slots = 0;
  int conflicts = 0;
  // int backOff = 1;

  // Build hash table.
  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* buildInput = &db->orders[i];
    if (buildInput->employeeManagerID != managerID) {
      continue;
    }
    int hashValue = hash(buildInput->salesDate, size);
    while (hashTable[hashValue].isOccupied) {
      conflicts++;
      hashValue = nextSlotLinear(hashValue, size);
      // hashValue = nextSlotExpo(hashValue, size, backOff);
      // hashValue = nextSlotRehashed(hashValue, size);
      // backOff = backOff * 2;
    }
    hashTable[hashValue].isOccupied = true;
    hashTable[hashValue].salesDate = buildInput->salesDate;
    hashTable[hashValue].employee = buildInput->employee;
    slots++;
    // backOff = 1;
  }
  // printf("BUILD slots used: %d, conflicts: %d, size: %ld\n", slots, conflicts,
  //       size);

  conflicts = 0;

  // Count matching tuples.
  int tuplesCount = 0;
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    struct ItemTuple* probeInput = &db->items[i];
    if (probeInput->price >= price) {
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
      hashValue = nextSlotLinear(hashValue, size);
      // hashValue = nextSlotExpo(hashValue, size, backOff);
      // hashValue = nextSlotRehashed(hashValue, size);
      // backOff = backOff * 2;
    }
    // backOff = 1;
  }
  // printf("PROBE conflicts: %d\n", conflicts);

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
