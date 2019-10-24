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
  int employeeManagerID;
};

struct StoresHashTableSlot {
  bool isOccupied;
  int managerID;
};

struct Indices {
  struct RLEDate* RLEDates;
  size_t RLEDatesCardinality;

  struct HashTableSlot* ordersHashTable;
  // int* primitiveRoots;
  // size_t* primitiveSizes;
  int orderCardinality;
};

// TODO: improve has function to something better (search online).
int hash(int value, int size) { return value % size; }

int hash2(int value, int size) { return (2 * value) % size; }

int nextSlotLinear(int currentSlot, int size) {
  return (currentSlot + 1) % size;
}

int nextSlotExpo(int currentSlot, int size, int backOff) {
  return (currentSlot + backOff) % size;
}

int nextSlotRehashed(int currentSlot, int size, int root) {
  if (currentSlot == 0) return root;
  return (currentSlot * root) % size;
}

int Query1(struct Database* db, int managerID, int price) {
  // TODO: implement partitioning.
  // TODO: change size of hash table.
  struct Indices* indices = db->indices;
  struct HashTableSlot* hashTableOrders = indices->ordersHashTable;
  size_t size = indices->orderCardinality;
  // int conflicts = 0;

  // Count matching tuples.
  int tuplesCount = 0;
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    struct ItemTuple* probeInput = &db->items[i];
    if (probeInput->price >= price) {
      continue;
    }
    int hashValue = hash(probeInput->salesDate * probeInput->employee, size);
    while (hashTableOrders[hashValue].isOccupied) {
      // Keep on going even if we find a match because there could be
      // duplicates.
      if (hashTableOrders[hashValue].salesDate == probeInput->salesDate &&
          hashTableOrders[hashValue].employee == probeInput->employee &&
          managerID == hashTableOrders[hashValue].employeeManagerID) {
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
  // Idea:
  // iterate through orders and binary search inside items how many
  // orders have been placed in the the relevant period.
  // Complexity:
  // - time: O(orders * log(items))
  // - space: O(1)
  //
  // Preprocessing:
  // - time: O(items * log(items)) [ or O(items) with counting sort ]
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
  struct Indices* indices = db->indices;
  struct HashTableSlot* hashTableOrders = indices->ordersHashTable;
  size_t size = indices->orderCardinality;
  // int root = 0;
  // root = root;
  // switch(db->ordersCardinality){
  //   case 256:
  //     size = indices->primitiveSizes[0];
  //     root = indices->primitiveRoots[0];
  //     break; 
  //   case 1024:
  //     size = indices->primitiveSizes[1];
  //     root = indices->primitiveRoots[1];
  //     break; 
  //   case 8192:
  //     size = indices->primitiveSizes[2];
  //     root = indices->primitiveRoots[2];
  //     break; 
  //   case 65536:
  //     size = indices->primitiveSizes[3];
  //     root = indices->primitiveRoots[3];
  //     break; 
  //   default:
  //     size = indices->primitiveSizes[4];
  //     root = indices->primitiveRoots[4]; 
  // } 

  // Build Stores hash table.
  size_t sizeStores = db->storesCardinality + 1;
  struct StoresHashTableSlot hashTableStores[sizeStores];
  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < sizeStores; i++) {
    hashTableStores[i].isOccupied = false;
  }

  // Populate Store hash table.
  for (size_t i = 0; i < db->storesCardinality; i++) {
    struct StoreTuple* buildInput = &db->stores[i];
    if (buildInput->countryID != countryID) {
      continue;
    }
    int hashValue = hash(buildInput->managerID, sizeStores);
    while (hashTableStores[hashValue].isOccupied) {
      hashValue = nextSlotLinear(hashValue, sizeStores);
      // hashValue = nextSlotExpo(hashValue, sizeStores, backOff);
      // hashValue = nextSlotRehashed(hashValue, sizeStores);
    }
    hashTableStores[hashValue].isOccupied = true;
    hashTableStores[hashValue].managerID = buildInput->managerID;
  }

  // Count tuples.
  int tupleCount = 0;

  for (size_t i = 0; i < db->itemsCardinality; i++) {
    struct ItemTuple* probeInput = &db->items[i];
    int hashValue = hash(probeInput->salesDate * probeInput->employee, size);
    // int backOffOrders = 1;
    // int conflictsOrders = 0;
    while (hashTableOrders[hashValue].isOccupied) {
      // Keep on going even if we find a match because there could be
      // duplicates.

      if (hashTableOrders[hashValue].salesDate == probeInput->salesDate &&
          hashTableOrders[hashValue].employee == probeInput->employee) {
        int hashValueStores =
            hash(hashTableOrders[hashValue].employeeManagerID, sizeStores);
        // int conflictsStores = 0;
        while (hashTableStores[hashValueStores].isOccupied) {
          if (hashTableStores[hashValueStores].managerID ==
              hashTableOrders[hashValue].employeeManagerID) {
            tupleCount++;
          }
          hashValueStores = nextSlotLinear(hashValueStores, sizeStores);
          // hashValueStores = nextSlotExpo(hashValueStores, sizeStores,
          // backOffStores); hashValueStores = nextSlotRehashed(hashValueStores,
          // sizeStores); conflictsStores++;
        }
      }

      hashValue = nextSlotLinear(hashValue, size);
      // hashValue = nextSlotExpo(hashValue, size, backOffOrders);
      // hashValue = nextSlotRehashed(hashValue, size, root);

      // backOffOrders = backOffOrders + conflictsOrders;
      // conflictsOrders++;
    }
    // conflictsCount = conflictsCount + conflictsOrders;
    // conflictsOrders = 0;
  }
  // printf("PROBE conflicts: %d\n", conflictsCount);

  return tupleCount;
}

// Comparison function for qsort.
int compare(const void* a, const void* b) { return *(int*)a - *(int*)b; }

// Compute RLEDates and return it, using a counting sort.
// Also populate RLEDatesCardinality.
// We cannot pass RLEDates as parameter as well because the compiler complains.
struct RLEDate* computeRLEDatesCountingSort(struct Database* db, size_t maximum,
                                            size_t* RLEDatesCardinality) {
  int* frequencyCount = malloc((maximum + 1) * sizeof(int));
  // Initialize frequencies to zero.
  for (size_t i = 0; i <= (size_t)maximum; i++) {
    frequencyCount[i] = 0;
  }
  // Count frequencies, and how many different values are there.
  size_t different = 0;
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    different += frequencyCount[db->items[i].salesDate] == 0;
    frequencyCount[db->items[i].salesDate]++;
  }

  // Compute Run-Length-Encoding with Length Prefix Summing.
  struct RLEDate* RLEDates = malloc(different * sizeof(struct RLEDate));
  size_t RLEIndex = 0;
  int prefixCount = 0;
  for (size_t i = 0; i <= (size_t)maximum; i++) {
    if (frequencyCount[i] > 0) {
      prefixCount += frequencyCount[i];
      RLEDates[RLEIndex].date = i;
      RLEDates[RLEIndex].prefixCount = prefixCount;
      RLEIndex++;
    }
  }
  free(frequencyCount);

  *RLEDatesCardinality = different;  // == RLEIndex.
  return RLEDates;
}

// Compute RLEDates and return it, using the stdlib sort.
// Also populate RLEDatesCardinality.
// We cannot pass RLEDates as parameter as well because the compiler complains.
struct RLEDate* computeRLEDatesQSort(struct Database* db,
                                     size_t* RLEDatesCardinality) {
  int* orderedItemSalesDate = malloc(db->itemsCardinality * sizeof(int));
  // Etract dates.
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    orderedItemSalesDate[i] = db->items[i].salesDate;
  }
  // Sort dates.
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
  
  free(orderedItemSalesDate);

  *RLEDatesCardinality = different;  // == RLEIndex.
  return RLEDates;
}

void CreateIndices(struct Database* db) {
  struct Indices* indices = malloc(sizeof(struct Indices));

  {
    // Create indexes for query 2.
    // Observation: usually there are many repeated dates, which means that
    // using a Run-Length-Encoding with Length Prefix Summing should help
    // compressing the data a lot.

    // Find out about how sparse the values are in order to use the appropriate
    // sorting function.
    int minimum = __INT_MAX__;
    int maximum = -1;
    for (size_t i = 0; i < db->itemsCardinality; i++) {
      if (db->items[i].salesDate < minimum) {
        minimum = db->items[i].salesDate;
      }
      if (db->items[i].salesDate > maximum) {
        maximum = db->items[i].salesDate;
      }
    }

    size_t RLEDatesCardinality;
    struct RLEDate* RLEDates;

    if ((size_t)(maximum - minimum) < db->itemsCardinality) {
      // Data are not very sparse, it makes sense to use counting sort.
      RLEDates = computeRLEDatesCountingSort(db, maximum, &RLEDatesCardinality);
    } else {
      // Use normal sort from stdlib.
      RLEDates = computeRLEDatesQSort(db, &RLEDatesCardinality);
    }

    indices->RLEDates = RLEDates;
    indices->RLEDatesCardinality = RLEDatesCardinality;
  }

  {
    // Create indices Order Hash Table for query 1 and 3.
    size_t size = db->ordersCardinality * 2;
    indices->orderCardinality = size;
    // int root = 0;
    // root = root;
    // int* primitiveRoots = malloc(5 * sizeof(int));
    // size_t* primitiveSizes = malloc(5 * sizeof(size_t));
    // primitiveRoots[0] = 7;
    // primitiveRoots[1] = 6;
    // primitiveRoots[2] = 17;
    // primitiveRoots[3] = 11;
    // primitiveRoots[4] = 11;
    // primitiveSizes[0] = 514;
    // primitiveSizes[1] = 2053;
    // primitiveSizes[2] = 16538;
    // primitiveSizes[3] = 131074;
    // primitiveSizes[4] = 524287;

    // indices->primitiveRoots = primitiveRoots;
    // indices->primitiveSizes = primitiveSizes;

    // switch (db->ordersCardinality) {
    //   case 256:
    //     size = primitiveSizes[0];
    //     root = primitiveRoots[0];
    //     break;
    //   case 1024:
    //     size = primitiveSizes[1];
    //     root = primitiveRoots[1];
    //     break;
    //   case 8192:
    //     size = primitiveSizes[2];
    //     root = primitiveRoots[2];
    //     break;
    //   case 65536:
    //     size = primitiveSizes[3];
    //     root = primitiveRoots[3];
    //     break;
    //   default:
    //     size = primitiveSizes[4];
    //     root = primitiveRoots[4];
    // }
    struct HashTableSlot* hashTableOrders =
        malloc(size * sizeof(struct HashTableSlot));
    indices->ordersHashTable = &hashTableOrders[0];
    size_t conflicts = 0;

    // TODO: find a way to remove this horrible hack.
    // Initialize all slots to be empty. Slow.
    for (size_t i = 0; i < size; i++) {
      hashTableOrders[i].isOccupied = false;
    }
    for (size_t i = 0; i < db->ordersCardinality; i++) {
      struct OrderTuple* buildInput = &db->orders[i];
      int hashValue = hash(buildInput->salesDate * buildInput->employee, size);
      while (hashTableOrders[hashValue].isOccupied) {
        hashValue = nextSlotLinear(hashValue, size);
        // hashValue = nextSlotExpo(hashValue, size, backOff);
        // hashValue = nextSlotRehashed(hashValue, size, root);
        // backOff = backOff + conflicts;
        conflicts++;
      }
      // conflictsCount = conflictsCount + conflicts;
      hashTableOrders[hashValue].isOccupied = true;
      hashTableOrders[hashValue].salesDate = buildInput->salesDate;
      hashTableOrders[hashValue].employee = buildInput->employee;
      hashTableOrders[hashValue].employeeManagerID =
          buildInput->employeeManagerID;

      // backOff = 1;
      // conflicts = 0;
    }
    // printf("orders conflicts: %ld\n", conflicts);
  }
  db->indices = indices;
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  struct Indices* indices = db->indices;
  free(indices->RLEDates);
  // free(indices->primitiveRoots);
  // free(indices->primitiveSizes);
  free(indices->ordersHashTable);
  free(indices);
  db->indices = NULL;
}