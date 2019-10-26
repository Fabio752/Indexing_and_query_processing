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

struct OrdersHashTableSlot {
  bool isOccupied;
  int salesDate;
  int employee;
};

struct StoresHashTableSlot {
  bool isOccupied;
  int managerID;
};

struct SalesDateEmployeeToCount {
  bool isOccupied;
  int salesDate;
  int employee;
  int count;
};

struct Indices {
  struct RLEDate* RLEDates;
  size_t RLEDatesCardinality;

  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality;
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
  // TODO: use some indexing to speed this up. E.g. maybe sort items by price?
  // E.g. maybe prebuild ordersHashTableSize?
  size_t ordersHashTableSize = db->ordersCardinality + 1;
  struct OrdersHashTableSlot* ordersHashTable =
      malloc(ordersHashTableSize * sizeof(struct OrdersHashTableSlot));

  // TODO: find a way to remove this horrible hack.
  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < ordersHashTableSize; i++) {
    ordersHashTable[i].isOccupied = false;
  }

  // Build orders hash table.
  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* orderTuple = &db->orders[i];
    if (orderTuple->employeeManagerID != managerID) {
      continue;
    }
    int hashValue =
        hash(orderTuple->salesDate * orderTuple->employee, ordersHashTableSize);
    while (ordersHashTable[hashValue].isOccupied) {
      hashValue = nextSlotLinear(hashValue, ordersHashTableSize);
    }
    ordersHashTable[hashValue].isOccupied = true;
    ordersHashTable[hashValue].salesDate = orderTuple->salesDate;
    ordersHashTable[hashValue].employee = orderTuple->employee;
  }

  // Count matching tuples.
  int tupleCount = 0;
  for (size_t i = 0; i < db->itemsCardinality; i++) {
    if (db->items[i].price >= price) {
      continue;
    }
    struct ItemTuple* itemTuple = &db->items[i];

    int hashValue =
        hash(itemTuple->salesDate * itemTuple->employee, ordersHashTableSize);
    while (ordersHashTable[hashValue].isOccupied) {
      if (ordersHashTable[hashValue].salesDate == itemTuple->salesDate &&
          ordersHashTable[hashValue].employee == itemTuple->employee) {
        tupleCount++;
      }
      hashValue = nextSlotLinear(hashValue, ordersHashTableSize);
    }
  }

  free(ordersHashTable);

  return tupleCount;
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
  struct Indices* indices = db->indices;
  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
      indices->salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality =
      indices->salesDateEmployeeToCountCardinality;

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

  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* orderTuple = &db->orders[i];
    int hashValueStores = hash(orderTuple->employeeManagerID, sizeStores);
    while (hashTableStores[hashValueStores].isOccupied) {
      if (hashTableStores[hashValueStores].managerID ==
          orderTuple->employeeManagerID) {
        // Lookup how many items matched this pair (salesDate, employee).
        int hashValueSalesDateEmployee =
            hash(orderTuple->salesDate * orderTuple->employee,
                 salesDateEmployeeToCountCardinality);
        while (
            salesDateEmployeeToCountHT[hashValueSalesDateEmployee].isOccupied) {
          if (salesDateEmployeeToCountHT[hashValueSalesDateEmployee]
                      .salesDate == orderTuple->salesDate &&
              salesDateEmployeeToCountHT[hashValueSalesDateEmployee].employee ==
                  orderTuple->employee) {
            tupleCount +=
                salesDateEmployeeToCountHT[hashValueSalesDateEmployee].count;
          }
          hashValueSalesDateEmployee = nextSlotLinear(
              hashValueSalesDateEmployee, salesDateEmployeeToCountCardinality);
        }
      }
      hashValueStores = nextSlotLinear(hashValueStores, sizeStores);
    }
  }

  return tupleCount >> 1;
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
    // TODO: this is weirdly slow.

    // Q3 indices.
    // Observations: we know that items.salesDate and items.employee are foreign
    // keys into orders. We also don't care about price in Q3. Idea: 1)
    // aggregate items, counting the number of different prices for each pair
    //    salesDate and employee. This table will have cardinality <=
    //    ordersCardinality.
    // 2) join the aggregated items with orders. This table will have
    //    cardinality <= ordersCardinality.
    // 3) Use this in Q3.
    // 4) Win the contest.
    // (maybe you can do step 1 and 2 together)

    // Create hash table from the pair (salesDate, employee) to count.
    // Each pair (salesDate, employee) comes from Orders, and the count is
    // initially set to zero.
    // TODO: find out whether salesDate, employee is enough as primary key for
    // orders. If so, we would have no duplictes in orders, and the lookup would
    // be simpler.
    size_t salesDateEmployeeToCountCardinality = db->ordersCardinality * 2;
    struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
        malloc(salesDateEmployeeToCountCardinality *
               sizeof(struct SalesDateEmployeeToCount));
    for (size_t i = 0; i < salesDateEmployeeToCountCardinality; i++) {
      salesDateEmployeeToCountHT[i].isOccupied = false;
    }
    for (size_t i = 0; i < db->ordersCardinality; i++) {
      struct OrderTuple* orderTuple = &db->orders[i];
      int hashValue = hash(orderTuple->salesDate * orderTuple->employee,
                           salesDateEmployeeToCountCardinality);
      while (salesDateEmployeeToCountHT[hashValue].isOccupied) {
        hashValue =
            nextSlotLinear(hashValue, salesDateEmployeeToCountCardinality);
      }
      // conflictsCount = conflictsCount + conflicts;
      // TODO: maybe use salesDate..[hashValue] = {true, ....}
      salesDateEmployeeToCountHT[hashValue].isOccupied = true;
      salesDateEmployeeToCountHT[hashValue].salesDate = orderTuple->salesDate;
      salesDateEmployeeToCountHT[hashValue].employee = orderTuple->employee;
      salesDateEmployeeToCountHT[hashValue].count = 0;
    }

    // Iterate through items to count how many rows have a particular pair
    // (salesDate, employee) that can be merged with orders.
    for (size_t i = 0; i < db->itemsCardinality; i++) {
      // TODO: change these to copies, not pointers. Maybe goes faster.
      struct ItemTuple* itemsTuple = &db->items[i];
      int hashValue = hash(itemsTuple->salesDate * itemsTuple->employee,
                           salesDateEmployeeToCountCardinality);
      while (salesDateEmployeeToCountHT[hashValue].isOccupied) {
        if (salesDateEmployeeToCountHT[hashValue].salesDate ==
                itemsTuple->salesDate &&
            salesDateEmployeeToCountHT[hashValue].employee ==
                itemsTuple->employee) {
          salesDateEmployeeToCountHT[hashValue].count++;
        }
        hashValue =
            nextSlotLinear(hashValue, salesDateEmployeeToCountCardinality);
      }
    }

    indices->salesDateEmployeeToCountHT = salesDateEmployeeToCountHT;
    indices->salesDateEmployeeToCountCardinality =
        salesDateEmployeeToCountCardinality;
  }

  db->indices = indices;
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  struct Indices* indices = db->indices;
  free(indices->RLEDates);
  free(indices->salesDateEmployeeToCountHT);
  free(indices);
  db->indices = NULL;
}
