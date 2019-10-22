#include <stdbool.h>
#include <stdlib.h>

#include "solution.h"

// DEBUG
#include <stdio.h>
#include <time.h>

struct RLEDate {
  int date;
  int prefixCount;
};

struct HashTableSlot {
  bool isOccupied;
  int salesDate;
  int employee;
};

struct StoresHashTableSlot {
  bool isOccupied;
  int managerID;
};

struct Indices {
  struct RLEDate* RLEDates;
  size_t RLEDatesCardinality;
};

// TODO: improve has function to something better (search online).
int hash(int value, int size) { return value % size; }

int nextSlotLinear(int currentSlot, int size) {
  return (currentSlot + 1) % size;
}
int nextSlotExpo(int currentSlot, int size, int backOff) {
  return (currentSlot + backOff) % size;
}
int nextSlotRehashed(int currentSlot, int size) {
  return ((currentSlot + 37) / 3 * 15) % size;
}

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
  // printf("BUILD slots used: %d, conflicts: %d, size: %ld\n", slots,
  // conflicts,
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

// TODO: improve by using sparse table.
// https://cp-algorithms.com/sequences/rmq.html
int Query2(struct Database* db, int discount, int date) {
  // Idea:
  // iterate through orders and binary search inside items how many
  // orders have been placed in the the relevant period.
  // Complexity:
  // - time: O(orders * log(items))
  // - space: O(1)
  //
  // Preprocessing:
  // - time: O(items * log(items))
  // - space: O(items)
  struct Indices* indices = db->indices;
  size_t RLEDatesCardinality = indices->RLEDatesCardinality;

  int tuplesCount = 0;
  for (size_t i = 0; i < db->ordersCardinality; i++) {
    if (db->orders[i].discount != discount) {
      continue;
    }
    int orderDate = db->orders[i].salesDate;
    // Binary search the lower bound. Find the smallest date
    // >= orderDate - date.
    int target = orderDate - date;
    size_t lb = 0, ub = RLEDatesCardinality;
    while (lb < ub) {
      size_t mid = (lb + ub) >> 1;
      if (target <= indices->RLEDates[mid].date) {
        ub = mid;
      } else {
        lb = mid + 1;
      }
    }
    // Result is at position lb.
    // Note that lb could be out-of-bound, which means that all items
    // salesDate are too small.
    if (lb == indices->RLEDatesCardinality) {
      continue;
    }
    size_t leftIdx = lb;

    // Binary search the upper bound. Find the biggest date
    // <= orderDate. (Actually, we find the one after).
    // Do not reset the lower bound to zero since the location we are looking
    // for is surely bigger or equal to the current one.
    lb -= lb != 0;
    ub = RLEDatesCardinality;
    while (lb < ub) {
      size_t mid = (lb + ub) >> 1;
      if (orderDate >= indices->RLEDates[mid].date) {
        lb = mid + 1;
      } else {
        ub = mid;
      }
    }
    // Result is at position lb.
    size_t rightIdx = lb;

    int leftVal = leftIdx == 0 ? 0 : indices->RLEDates[leftIdx - 1].prefixCount;
    int rightVal =
        rightIdx == 0 ? 0 : indices->RLEDates[rightIdx - 1].prefixCount;

    tuplesCount += rightVal - leftVal;
  }
  return tuplesCount;
}

int Query3(struct Database* db, int countryID) {
  // Build Items hash table.
  // TODO: implement partitioning.
  // TODO: change size of hash table.
  size_t size = db->itemsCardinality * 2;
  struct HashTableSlot hashTableItems[size];
  
  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < size; i++) {
    hashTableItems[i].isOccupied = false;
  }
  clock_t start = clock();
  int conflictsCount = 0;
  int backOff = 1;
  int conflicts = 0;
  
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    struct ItemTuple* buildInput = &db->items[i];
    int hashValue = hash(buildInput->salesDate, size);
    while (hashTableItems[hashValue].isOccupied) {
      // hashValue = nextSlotLinear(hashValue, size);
      hashValue = nextSlotExpo(hashValue, size, backOff);
      // hashValue = nextSlotRehashed(hashValue, size);
      backOff = backOff + conflicts;
      conflicts++;
      printf("backoff = %d\n", backOff);
    }
    conflictsCount = conflictsCount + conflicts;
    hashTableItems[hashValue].isOccupied = true;
    hashTableItems[hashValue].salesDate = buildInput->salesDate;
    hashTableItems[hashValue].employee = buildInput->employee;
    backOff = 1;
    conflicts = 0;
  }
  double time_taken = (clock() - start) / 1000000.0; 
  time_taken = time_taken; // Because it gives a warning if not used.
  printf("building hash table hashTableItems in sec: %f\n", time_taken);
  printf("items conflicts: %d\n", conflictsCount);

  // Build Stores hash table.
  size_t sizeStores = db->storesCardinality + 1;
  struct StoresHashTableSlot hashTableStores[sizeStores];
  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < sizeStores; i++) {
    hashTableStores[i].isOccupied = false;
  }
  start = clock();
  
  conflictsCount = 0;
  
  // Build hash table.
  for (size_t i = 0; i < db->storesCardinality; i++) {
    struct StoreTuple* buildInput = &db->stores[i];
    if (buildInput->countryID != countryID) {
      continue;
    }
    int hashValue = hash(buildInput->managerID, sizeStores);
    while (hashTableStores[hashValue].isOccupied) {
      // hashValue = nextSlotLinear(hashValue, sizeStores);
      hashValue = nextSlotExpo(hashValue, sizeStores, backOff);
      // hashValue = nextSlotRehashed(hashValue, sizeStores);
      backOff = backOff + conflicts;
      conflicts++;
      printf("backoff = %d\n", backOff);
    }
    conflictsCount = conflictsCount + conflicts;
    hashTableStores[hashValue].isOccupied = true;
    hashTableStores[hashValue].managerID = buildInput->managerID;
    backOff = 1;
    conflicts = 0;
  }
  time_taken = (clock() - start) / 1000000.0;
  // printf("building hash table hashTableStores in sec: %f\n", time_taken);
  // printf("stores conflicts: %d\n", conflictsCount);

  // Count tuples.
  int tupleCount = 0;

  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* probeInput = &db->orders[i];
    int hashValue = hash(probeInput->salesDate, size);
    int backOffOrders = 1;
    int conflictsOrders = 0;
    while (hashTableItems[hashValue].isOccupied) {
      // Keep on going even if we find a match because there could be
      // duplicates.
      if (hashTableItems[hashValue].salesDate == probeInput->salesDate &&
          hashTableItems[hashValue].employee == probeInput->employee) {
        int hashValueStores = hash(probeInput->employeeManagerID, sizeStores);
        int backOffStores = 1;
        int conflictsStores = 0;
        while (hashTableStores[hashValueStores].isOccupied) {
          if (hashTableStores[hashValueStores].managerID ==
              probeInput->employeeManagerID) {
            tupleCount++;
          }
          // hashValueStores = nextSlotLinear(hashValueStores, sizeStores);
          hashValueStores = nextSlotExpo(hashValueStores, sizeStores, backOffStores);
          // hashValueStores = nextSlotRehashed(hashValueStores, sizeStores);
          backOffStores = backOffStores + conflictsStores;
          conflictsStores++;
        }
        conflictsCount = conflictsCount + conflictsStores;
        conflictsStores = 0;
      }
      // hashValue = nextSlotLinear(hashValue, size);
      hashValue = nextSlotExpo(hashValue, size, backOffOrders);
      // hashValue = nextSlotRehashed(hashValue, size);
      backOffOrders = backOffOrders + conflictsOrders;
      conflictsOrders++;
    }
    conflictsCount = conflictsCount + conflictsOrders;
    conflictsOrders = 0;
  }
  printf("PROBE conflicts: %d\n", conflictsCount);

  return tupleCount;
}


// Comparison function for qsort.
int compare(const void* a, const void* b) { return *(int*)a - *(int*)b; }

void CreateIndices(struct Database* db) {
  struct Indices* indices = malloc(sizeof(struct Indices));

  // Create indexes for query 2.
  // Observation: usually there are many repeated dates, which means that using
  // a Run-Length-Encoding with Length Prefix Summing should help compressing
  // the data a lot.
  int orderedItemSalesDate[db->itemsCardinality];
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    orderedItemSalesDate[i] = db->items[i].salesDate;
  }
  // FIXME: Slow!!
  qsort(orderedItemSalesDate, db->itemsCardinality, sizeof(int), compare);

  // Find how many different elements are there.
  size_t different = 1;
  for (size_t i = 1; i < db->itemsCardinality; i++) {
    if (orderedItemSalesDate[i] > orderedItemSalesDate[i - 1]) {
      different++;
    }
  }

  // Compute Run-Length-Encoding with Length Prefix Summing.
  struct RLEDate* RLEDates = malloc(different * sizeof(struct RLEDate));
  size_t RLEIndex = 0;
  for (size_t i = 1; i < db->itemsCardinality; i++) {
    if (orderedItemSalesDate[i] > orderedItemSalesDate[i - 1]) {
      RLEDates[RLEIndex].date = orderedItemSalesDate[i - 1];
      RLEDates[RLEIndex].prefixCount = i;
      RLEIndex++;
    }
  }
  // Add last elements.
  RLEDates[RLEIndex].date = orderedItemSalesDate[db->itemsCardinality - 1];
  RLEDates[RLEIndex].prefixCount = db->itemsCardinality;
  indices->RLEDates = RLEDates;
  indices->RLEDatesCardinality = RLEIndex + 1;

  db->indices = indices;
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  struct Indices* indices = db->indices;
  free(indices->RLEDates);
  free(indices);
  db->indices = NULL;
}
