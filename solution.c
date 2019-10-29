#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

#include "solution.h"

// DEBUG
#include <time.h>

#define NUMBER_OF_THREADS 4 // We have 4 cores.

struct RLEDate {
  int date;
  int prefixCount;
};

struct OrdersHashTableSlot {
  int count;  // Negative count means emply slot.
  int salesDate;
  int employee;
};

struct SalesDateEmployeeToCount {
  int count;  // Negative count means empty slot.
  int salesDate;
  int employee;
};

struct ThreadData {
  struct OrdersHashTableSlot* ordersHashTable;
  struct OrderTuple* partitionOrder;
  struct ItemTuple* partitionItem;
  size_t partitionedOrderCardinality;
  size_t partitionedItemCardinality;
  size_t singleOrderSize;
  size_t singleItemSize;
  int threadNumber;
  int result;
  pthread_t tid;
};

struct Indices {
  struct RLEDate* RLEDates;
  size_t RLEDatesCardinality;

  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality;
};

// TODO: improve has function to something better (search online).
int hash(int value, int size) { return value % size; }

int hash2(int value1, int value2, int size) {
  return ((223 + value1) * (47 + value2)) % size;
}

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

void* Q1BuildOrders(void* args) {
  // Build the four hash tables.
  
  struct ThreadData* threadData = (struct ThreadData*)args;
  size_t ordersHashTableSize = 2 * threadData->partitionedOrderCardinality; 
  if(ordersHashTableSize == 0)
    return NULL;
  
  printf("orderht size= %ld\n", ordersHashTableSize);
  
  struct OrdersHashTableSlot* ordersHashTable = 
    malloc(ordersHashTableSize * sizeof(struct OrdersHashTableSlot));
  if (ordersHashTable == NULL) {
    exit(1);
  }
  threadData->ordersHashTable = &ordersHashTable[0];
  
  // Initialize all slots to be empty. Slow.
  for(size_t i = 0; i < ordersHashTableSize; i++){
    ordersHashTable[i].count = -1;
  }  
  for (size_t i = 0; i < threadData->singleOrderSize; i++) {
    if (i >= threadData->partitionedOrderCardinality){
      printf("i = %ld\n", i);
      break;
    }
    struct OrderTuple* orderTuple = &threadData->partitionOrder[i];
    int hashValue = 
        hash(orderTuple->salesDate * orderTuple->employee, ordersHashTableSize);
    while (ordersHashTable[hashValue].count >= 0) {
      if (ordersHashTable[hashValue].salesDate == orderTuple->salesDate &&
          ordersHashTable[hashValue].employee == orderTuple->employee) {
        // We already have inserted the pair (salesDate, employee) in the
        // table, so we don't need to add it again. This also guarantees the
        // uniqueness in the keys of the table.
        ordersHashTable[hashValue].count++;
        break;
      }
      hashValue = nextSlotLinear(hashValue, ordersHashTableSize);
    }
    if (ordersHashTable[hashValue].count < 0) {
      // Add new (salesDate, employee) pair.
      ordersHashTable[hashValue].count = 1;
      ordersHashTable[hashValue].salesDate = orderTuple->salesDate;
      ordersHashTable[hashValue].employee = orderTuple->employee;
    }
  }
  threadData->ordersHashTable = &ordersHashTable[0];

  return NULL;
}

void* Q1ProbeOrders(void* args) {
  // Count matching tuples.
  int result = 0;
  struct ThreadData* threadData = (struct ThreadData*)args;
  
  for (size_t i = 0; i < threadData->singleItemSize; i++) {
    if (i >= threadData->partitionedItemCardinality) {
      break;
    }
    struct ItemTuple* itemTuple = &threadData->partitionItem[i];

    int hashValue =
        hash(itemTuple->salesDate * itemTuple->employee, 2 * threadData->partitionedOrderCardinality);
    while (threadData->ordersHashTable[hashValue].count >= 0) {
      if (threadData->ordersHashTable[hashValue].salesDate == itemTuple->salesDate &&
          threadData->ordersHashTable[hashValue].employee == itemTuple->employee) {
        result += threadData->ordersHashTable[hashValue].count;
        break;
      }
      hashValue = nextSlotLinear(hashValue, 2 * threadData->partitionedOrderCardinality);
    }
  }
  threadData->result = result;
  return NULL;
}

int Query1(struct Database* db, int managerID, int price) {
  // TODO: use some indexing to speed this up. E.g. maybe sort items by price?
  // E.g. maybe prebuild ordersHashTableSize?
  
  // Partitioning Order table.
  struct ThreadData threadData[NUMBER_OF_THREADS];
  size_t partitionedOrderCardinality = (2 * db->ordersCardinality) / NUMBER_OF_THREADS;
  size_t indexesOrder[NUMBER_OF_THREADS];
  
  struct OrderTuple** partitionedOrder = malloc(4 * sizeof(struct OrderTuple));
  for(int i = 0; i < 4; i++){
    indexesOrder[i] = 0;
    partitionedOrder[i] = malloc(partitionedOrderCardinality * sizeof(struct OrderTuple));
  }

  int tableOffset;
  size_t nextEmptySlot;
  for(size_t i = 0; i < db->ordersCardinality; i++){
    if(db->orders[i].employeeManagerID != managerID){
      continue;
    }
    tableOffset = (db->orders[i].salesDate + db->orders[i].employee) % NUMBER_OF_THREADS;
    nextEmptySlot = indexesOrder[tableOffset]; 
    partitionedOrder[tableOffset][nextEmptySlot].employee = db->orders[i].employee;
    partitionedOrder[tableOffset][nextEmptySlot].salesDate = db->orders[i].salesDate;
    indexesOrder[tableOffset]++;
  }
  for(int i = 0 ; i < 4; i++){
    printf("position %d, index %ld\n", i, indexesOrder[i]);
  }

  // Partitioning Item table.
  size_t partitionedItemCardinality = (2 * db->itemsCardinality) / NUMBER_OF_THREADS;
  size_t indexesItem[NUMBER_OF_THREADS];
  
  struct ItemTuple** partitionedItem = malloc(4 * sizeof(struct OrderTuple));
  for(int i = 0; i < 4; i++){
    indexesItem[i] = 0;
    partitionedItem[i] = malloc(partitionedItemCardinality * sizeof(struct OrderTuple));
  }

  for(size_t i = 0; i < db->itemsCardinality; i++){
    if(db->items[i].price >= price){
      continue;
    }
    tableOffset = (db->items[i].salesDate + db->items[i].employee) % NUMBER_OF_THREADS;
    nextEmptySlot = indexesItem[tableOffset]; 
    partitionedItem[tableOffset][nextEmptySlot].employee = db->items[i].employee;
    partitionedItem[tableOffset][nextEmptySlot].salesDate = db->items[i].salesDate;
    indexesItem[tableOffset]++;
  }
  
  for(int i = 0 ; i < 4; i++){
    printf("position %d, index %ld\n", i, indexesItem[i]);
  }

  // Parallelise probing using threads.
  // Split the ranges.
  for (size_t i = 0; i < NUMBER_OF_THREADS; i++) {
    threadData[i] = (struct ThreadData){
        .partitionOrder = partitionedOrder[i], // Shared.
        .partitionItem = partitionedItem[i], // Shared.
        .partitionedOrderCardinality = indexesOrder[i],
        .partitionedItemCardinality = indexesItem[i],
        .singleOrderSize = partitionedOrderCardinality,
        .singleItemSize = partitionedItemCardinality,
        .threadNumber = i };
  }

  // Start threads.
  for (int i = 0; i < NUMBER_OF_THREADS; i++) {
    if(indexesOrder[i] > 0 && indexesItem[i] > 0){
      printf("start build call\n");
      pthread_create(&threadData[i].tid, NULL, Q1BuildOrders, &threadData[i]);
      printf("finish build call\n");
    }
  }
  for (int i = 0; i < NUMBER_OF_THREADS; i++){
    if(indexesOrder[i] > 0 && indexesItem[i] > 0){
      printf("start probe call\n");
      pthread_create(&threadData[i].tid, NULL, Q1ProbeOrders, &threadData[i]);
      printf("finish probe call\n");
    }
  }
  
  int tuplesCount = 0;
  // Join threads.
  for (int i = 0; i < NUMBER_OF_THREADS; i++) {
    if(indexesOrder[i] > 0 && indexesItem[i] > 0){
      pthread_join(threadData[i].tid, NULL);
      tuplesCount += threadData[i].result;
    }
    free(partitionedOrder[i]);
    free(partitionedItem[i]);
  }

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
  struct Indices* indices = db->indices;
  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
      indices->salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality =
      indices->salesDateEmployeeToCountCardinality;

  // Build Stores hash table.
  // The only hashed value is the employeeManagerID. If negative, the slot is
  // empty.
  size_t sizeStores = db->storesCardinality + 1;
  int* hashTableStores = malloc(sizeStores * sizeof(int));
  if (hashTableStores == NULL) {
    exit(1);
  }

  // Initialize all slots to be empty. Slow.
  for (size_t i = 0; i < sizeStores; i++) {
    hashTableStores[i] = -1;
  }
  // Populate Store hash table.
  for (size_t i = 0; i < db->storesCardinality; i++) {
    struct StoreTuple* buildInput = &db->stores[i];
    if (buildInput->countryID != countryID) {
      continue;
    }
    int hashValue = hash(buildInput->managerID, sizeStores);
    while (hashTableStores[hashValue] >= 0) {
      hashValue = nextSlotLinear(hashValue, sizeStores);
      // hashValue = nextSlotExpo(hashValue, sizeStores, backOff);
      // hashValue = nextSlotRehashed(hashValue, sizeStores);
    }
    hashTableStores[hashValue] = buildInput->managerID;
  }

  // Count tuples.
  int tupleCount = 0;

  for (size_t i = 0; i < db->ordersCardinality; i++) {
    struct OrderTuple* orderTuple = &db->orders[i];
    int hashValueStores = hash(orderTuple->employeeManagerID, sizeStores);
    while (hashTableStores[hashValueStores] >= 0) {
      if (hashTableStores[hashValueStores] == orderTuple->employeeManagerID) {
        // Lookup how many items matched this pair (salesDate, employee).
        int hashValueSalesDateEmployee =
            hash2(orderTuple->salesDate, orderTuple->employee,
                  salesDateEmployeeToCountCardinality);
        // TODO: change these condition to have == instead of !=. Usually ==
        // evaluates to false earlier.
        while (
            salesDateEmployeeToCountHT[hashValueSalesDateEmployee].count >= 0 &&
            (salesDateEmployeeToCountHT[hashValueSalesDateEmployee].salesDate !=
                 orderTuple->salesDate ||
             salesDateEmployeeToCountHT[hashValueSalesDateEmployee].employee !=
                 orderTuple->employee)) {
          hashValueSalesDateEmployee = nextSlotLinear(
              hashValueSalesDateEmployee, salesDateEmployeeToCountCardinality);
        }
        if (salesDateEmployeeToCountHT[hashValueSalesDateEmployee].count >= 0) {
          tupleCount +=
              salesDateEmployeeToCountHT[hashValueSalesDateEmployee].count;
        }
      }
      hashValueStores = nextSlotLinear(hashValueStores, sizeStores);
    }
  }
  free(hashTableStores);

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
  if (frequencyCount == NULL) {
    exit(1);
  }
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
  if (RLEDates == NULL) {
    exit(1);
  }
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
  if (orderedItemSalesDate == NULL) {
    exit(1);
  }
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
  if (RLEDates == NULL) {
    exit(1);
  }
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
  if (indices == NULL) {
    exit(1);
  }

  {
    // Create indices for query 2.
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
    // keys into orders. We also don't care about price in Q3. Idea:
    // 1) aggregate items, counting the number of different prices for each pair
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
    size_t salesDateEmployeeToCountCardinality = db->ordersCardinality * 2;
    struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
        malloc(salesDateEmployeeToCountCardinality *
               sizeof(struct SalesDateEmployeeToCount));
    if (salesDateEmployeeToCountHT == NULL) {
      exit(1);
    }
    for (size_t i = 0; i < salesDateEmployeeToCountCardinality; i++) {
      salesDateEmployeeToCountHT[i].count = -1;
    }
    for (size_t i = 0; i < db->ordersCardinality; i++) {
      struct OrderTuple* orderTuple = &db->orders[i];
      int hashValue = hash2(orderTuple->salesDate, orderTuple->employee,
                            salesDateEmployeeToCountCardinality);
      while (salesDateEmployeeToCountHT[hashValue].count >= 0) {
        if (salesDateEmployeeToCountHT[hashValue].salesDate ==
                orderTuple->salesDate &&
            salesDateEmployeeToCountHT[hashValue].employee ==
                orderTuple->employee) {
          // We already have inserted the pair (salesDate, employee) in the
          // table, so we don't need to add it again. This also guarantees the
          // uniqueness in the keys of the table.
          break;
        }
        hashValue =
            nextSlotLinear(hashValue, salesDateEmployeeToCountCardinality);
      }
      if (salesDateEmployeeToCountHT[hashValue].count < 0) {
        // Adding new pair (salesDate, employee).
        salesDateEmployeeToCountHT[hashValue].count = 0;
        salesDateEmployeeToCountHT[hashValue].salesDate = orderTuple->salesDate;
        salesDateEmployeeToCountHT[hashValue].employee = orderTuple->employee;
      }
    }
    // Iterate through items to count how many rows have a particular pair
    // (salesDate, employee) that can be merged with orders.
    for (size_t i = 0; i < db->itemsCardinality; i++) {
      struct ItemTuple* itemsTuple = &db->items[i];
      int hashValue = hash2(itemsTuple->salesDate, itemsTuple->employee,
                            salesDateEmployeeToCountCardinality);
      while (salesDateEmployeeToCountHT[hashValue].count >= 0 &&
             (salesDateEmployeeToCountHT[hashValue].salesDate !=
                  itemsTuple->salesDate ||
              salesDateEmployeeToCountHT[hashValue].employee !=
                  itemsTuple->employee)) {
        hashValue =
            nextSlotLinear(hashValue, salesDateEmployeeToCountCardinality);
      }
      if (salesDateEmployeeToCountHT[hashValue].count >= 0) {
        salesDateEmployeeToCountHT[hashValue].count++;
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
