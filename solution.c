#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "solution.h"

// DEBUG
#include <time.h>

#define NUMBER_OF_THREADS 4  // We have 4 cores.
#define NUMBER_OF_BINS 8

struct CompressedItem {
  uint16_t salesDate;  // Max is 16384.
  uint16_t employee;   // Max is 10752.
  int price;
};

struct RLEDate {
  uint16_t date;  // Max is 16384.
  int prefixCount;
};

struct OrdersHashTableSlot {
  int count;           // Zero count means emply slot.
  uint16_t salesDate;  // Max is 16384.
  uint16_t employee;   // Max is 10752.
};

struct SalesDateEmployeeToCount {
  int count;           // Zero count means empty slot.
  uint16_t salesDate;  // Max is 16384.
  uint16_t employee;   // Max is 10752.
};

struct Indices {
  struct CompressedItem** itemBins;
  int* binSizes;

  struct RLEDate* RLEDates;
  size_t RLEDatesCardinality;

  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality;
  size_t sizeStores;
};

struct ThreadDataQ1 {
  struct Database* db;
  struct OrdersHashTableSlot* ordersHashTable;
  size_t ordersHashTableSize;
  int price;  // -1 means all prices in the bins are safe.
  size_t startBin;
  size_t endBin;
  int result;
  pthread_t tid;
};

struct ThreadDataQ2 {
  struct Database* db;
  int discount;
  int date;
  size_t start;
  size_t end;
  int result;
  pthread_t tid;
};

struct ThreadDataQ3 {
  struct Database* db;
  int16_t* hashTableStores;  // Contains employeeManagerID, with max 1344.
  size_t sizeStores;
  size_t start;
  size_t end;
  int result;
  pthread_t tid;
};

struct ThreadDataBuildIndex {
  struct Database* db;
  pthread_t tid;
};

int hash(int value, int size) { return value & (size - 1); }

int hash2(int value1, int value2, int size) {
  return ((223 + value1) * (47 + value2)) & (size - 1);
}

int nextSlotLinear(int currentSlot, int size) {
  return (currentSlot + 1) & (size - 1);
}

void* Q1ProbeOrders(void* args) {
  // Count matching tuples.
  struct ThreadDataQ1 threadData = *((struct ThreadDataQ1*)args);
  struct Database* db = threadData.db;
  struct Indices* indices = db->indices;

  int result = 0;
  // Iterate through all relevant bins.
  for (size_t binIndex = threadData.startBin; binIndex <= threadData.endBin;
       ++binIndex) {
    struct CompressedItem* bin = indices->itemBins[binIndex];
    int binSize = indices->binSizes[binIndex];

    // Iterate through the bin.
    for (int i = 0; i < binSize; ++i) {
      if (threadData.price != -1 && bin[i].price >= threadData.price) {
        continue;
      }
      struct CompressedItem* itemTuple = &bin[i];

      int hashValue = hash(itemTuple->salesDate + itemTuple->employee,
                           threadData.ordersHashTableSize);
      while (threadData.ordersHashTable[hashValue].count > 0 &&
             !(threadData.ordersHashTable[hashValue].salesDate ==
                   itemTuple->salesDate &&
               threadData.ordersHashTable[hashValue].employee ==
                   itemTuple->employee)) {
        hashValue = nextSlotLinear(hashValue, threadData.ordersHashTableSize);
      }
      result += threadData.ordersHashTable[hashValue].count;
    }
  }
  ((struct ThreadDataQ1*)args)->result = result;

  return NULL;
}

int Query1(struct Database* db, int managerID, int price) {
  // Initialize orders hash table.
  size_t ordersHashTableSize = db->ordersCardinality;
  struct OrdersHashTableSlot* ordersHashTable =
      calloc(ordersHashTableSize, sizeof(struct OrdersHashTableSlot));
  if (ordersHashTable == NULL) {
    exit(1);
  }

  // Build orders hash table.
  for (size_t i = 0; i < db->ordersCardinality; ++i) {
    struct OrderTuple* orderTuple = &db->orders[i];
    if (orderTuple->employeeManagerID != managerID) {
      continue;
    }
    int hashValue =
        hash(orderTuple->salesDate + orderTuple->employee, ordersHashTableSize);
    while (ordersHashTable[hashValue].count > 0 &&  // Slot occupied.
           !(ordersHashTable[hashValue].salesDate == orderTuple->salesDate &&
             ordersHashTable[hashValue].employee == orderTuple->employee)) {
      hashValue = nextSlotLinear(hashValue, ordersHashTableSize);
    }
    if (ordersHashTable[hashValue].count == 0) {
      // Add new (salesDate, employee) pair.
      ordersHashTable[hashValue].salesDate = orderTuple->salesDate;
      ordersHashTable[hashValue].employee = orderTuple->employee;
    }
    ++ordersHashTable[hashValue].count;
  }

  // Find out how many bins are relevant.
  int lastBinIndex = price / ((db->itemsCardinality / 2) / (NUMBER_OF_BINS));

  int tuplesCount = 0;

  if (lastBinIndex < NUMBER_OF_THREADS) {
    // If we have got 4 or less bins to explore, use one thread for each.
    struct ThreadDataQ1 threadData[lastBinIndex + 1];
    for (int i = 0; i <= lastBinIndex; i++) {
      threadData[i] = (struct ThreadDataQ1){
          .db = db,                            // Shared.
          .ordersHashTable = ordersHashTable,  // Shared.
          .ordersHashTableSize = ordersHashTableSize,
          .price = (i == lastBinIndex)
                       ? price
                       : -1,  // Only last bin has to check for price.
          .startBin = i,
          .endBin = i};
      pthread_create(&threadData[i].tid, NULL, Q1ProbeOrders, &threadData[i]);
    }

    for (int i = 0; i <= lastBinIndex; ++i) {
      pthread_join(threadData[i].tid, NULL);
      tuplesCount += threadData[i].result;
    }
  } else {
    // Assign multiple bins to each thread. We know that we will have max 2
    // bins per thread.
    struct ThreadDataQ1 threadData[NUMBER_OF_THREADS];
    int curBin = 0;
    int threadsWithExtraBin = (lastBinIndex % NUMBER_OF_THREADS) + 1;
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
      threadData[i] =
          (struct ThreadDataQ1){.db = db,                            // Shared.
                                .ordersHashTable = ordersHashTable,  // Shared.
                                .ordersHashTableSize = ordersHashTableSize,
                                .price = price,
                                .startBin = curBin,
                                .endBin = curBin + (i < threadsWithExtraBin)};
      curBin += (i < threadsWithExtraBin) + 1;
      pthread_create(&threadData[i].tid, NULL, Q1ProbeOrders, &threadData[i]);
    }

    for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
      pthread_join(threadData[i].tid, NULL);
      tuplesCount += threadData[i].result;
    }
  }

  free(ordersHashTable);

  return tuplesCount;
}

void* Q2ProbeOrders(void* args) {
  struct ThreadDataQ2* threadData = (struct ThreadDataQ2*)args;
  struct Database* db = threadData->db;
  struct Indices* indices = db->indices;
  size_t RLEDatesCardinality = indices->RLEDatesCardinality;
  int discount = threadData->discount;
  int date = threadData->date;

  int tuplesCount = 0;
  for (size_t i = threadData->start; i < threadData->end; ++i) {
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
    // Use heuristic to make the initial upper bound as small as possible.
    lb -= lb != 0;
    ub = lb + date + 2 > RLEDatesCardinality ? RLEDatesCardinality
                                             : lb + date + 2;
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
  threadData->result = tuplesCount;

  return NULL;
}

int Query2(struct Database* db, int discount, int date) {
  // Idea:
  // iterate through orders and binary search inside items how many
  // orders have been placed in the the relevant period.

  // Parallelize probing using threads.
  struct ThreadDataQ2 threadData[NUMBER_OF_THREADS];
  // Split the ranges.
  for (size_t i = 0; i < NUMBER_OF_THREADS; ++i) {
    threadData[i] = (struct ThreadDataQ2){
        .db = db,  // Shared.
        .discount = discount,
        .date = date,
        .start = i * db->ordersCardinality / NUMBER_OF_THREADS,
        .end = (i + 1) * db->ordersCardinality / NUMBER_OF_THREADS};
  }
  threadData[NUMBER_OF_THREADS - 1].end =
      db->ordersCardinality;  // Make sure we cover the entire range.

  // Start threads.
  for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
    pthread_create(&threadData[i].tid, NULL, Q2ProbeOrders, &threadData[i]);
  }

  int tuplesCount = 0;
  // Join threads.
  for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
    pthread_join(threadData[i].tid, NULL);
    tuplesCount += threadData[i].result;
  }

  return tuplesCount;
}

void* Q3ProbeOrders(void* args) {
  struct ThreadDataQ3* threadData = (struct ThreadDataQ3*)args;
  struct Database* db = threadData->db;
  struct Indices* indices = db->indices;
  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
      indices->salesDateEmployeeToCountHT;
  size_t salesDateEmployeeToCountCardinality =
      indices->salesDateEmployeeToCountCardinality;
  int16_t* hashTableStores = threadData->hashTableStores;
  size_t sizeStores = threadData->sizeStores;

  // Count tuples.
  int tuplesCount = 0;

  // Iterate through a piece of orders, counting relevant tuples.
  for (size_t i = threadData->start; i < threadData->end; ++i) {
    struct OrderTuple* orderTuple = &db->orders[i];
    int hashValueStores = hash(orderTuple->employeeManagerID, sizeStores);
    // Verify this orderTuple can be joined with stores.
    while (hashTableStores[hashValueStores] >= 0) {
      if (hashTableStores[hashValueStores] == orderTuple->employeeManagerID) {
        // The orderTuple can be joined with stores.
        // Now lookup how many items matched this pair (salesDate, employee).
        int hashValueSalesDateEmployee =
            hash2(orderTuple->salesDate, orderTuple->employee,
                  salesDateEmployeeToCountCardinality);
        while (!(
            salesDateEmployeeToCountHT[hashValueSalesDateEmployee].salesDate ==
                orderTuple->salesDate &&
            salesDateEmployeeToCountHT[hashValueSalesDateEmployee].employee ==
                orderTuple->employee)) {
          hashValueSalesDateEmployee = nextSlotLinear(
              hashValueSalesDateEmployee, salesDateEmployeeToCountCardinality);
        }
        tuplesCount +=
            salesDateEmployeeToCountHT[hashValueSalesDateEmployee].count;
      }
      hashValueStores = nextSlotLinear(hashValueStores, sizeStores);
    }
  }
  threadData->result = tuplesCount;

  return NULL;
}

int Query3(struct Database* db, int countryID) {
  // Build Stores hash table.
  // The only hashed value is the employeeManagerID. If negative, the slot is
  // empty.
  struct Indices* indices = db->indices;
  size_t sizeStores = indices->sizeStores;
  int16_t* hashTableStores = malloc(sizeStores * sizeof(int16_t));
  if (hashTableStores == NULL) {
    exit(1);
  }

  // Initialize all slots to be empty.
  memset(hashTableStores, -1, sizeStores * sizeof(int16_t));

  // Populate Stores hash table.
  for (size_t i = 0; i < db->storesCardinality; ++i) {
    struct StoreTuple* buildInput = &db->stores[i];
    if (buildInput->countryID != countryID) {
      continue;
    }
    int hashValue = hash(buildInput->managerID, sizeStores);
    while (hashTableStores[hashValue] >= 0) {
      hashValue = nextSlotLinear(hashValue, sizeStores);
    }
    hashTableStores[hashValue] = buildInput->managerID;
  }

  // Parallelize probing using threads.
  struct ThreadDataQ3 threadData[NUMBER_OF_THREADS];
  // Split the ranges.
  for (size_t i = 0; i < NUMBER_OF_THREADS; ++i) {
    threadData[i] = (struct ThreadDataQ3){
        .db = db,                            // Shared.
        .hashTableStores = hashTableStores,  // Shared.
        .sizeStores = sizeStores,
        .start = i * db->ordersCardinality / NUMBER_OF_THREADS,
        .end = (i + 1) * db->ordersCardinality / NUMBER_OF_THREADS};
  }
  threadData[NUMBER_OF_THREADS - 1].end =
      db->ordersCardinality;  // Make sure we cover the entire range.

  // Start threads.
  for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
    pthread_create(&threadData[i].tid, NULL, Q3ProbeOrders, &threadData[i]);
  }

  int tuplesCount = 0;
  // Join threads.
  for (int i = 0; i < NUMBER_OF_THREADS; ++i) {
    pthread_join(threadData[i].tid, NULL);
    tuplesCount += threadData[i].result;
  }

  free(hashTableStores);

  return tuplesCount;
}

// Comparison function for qsort.
int compare(const void* a, const void* b) { return *(int*)a - *(int*)b; }

// Compute RLEDates and return it, using a counting sort.
// Also populate RLEDatesCardinality.
// We cannot pass RLEDates as parameter as well because the compiler complains.
struct RLEDate* computeRLEDatesCountingSort(struct Database* db, size_t maximum,
                                            size_t* RLEDatesCardinality) {
  // Prepare for counting sort.
  int* frequencyCount = calloc((maximum + 1), sizeof(int));
  if (frequencyCount == NULL) {
    exit(1);
  }

  // Count frequencies, and how many different values are there.
  size_t different = 0;
  for (size_t i = 0; i < db->itemsCardinality; ++i) {
    different += frequencyCount[db->items[i].salesDate] == 0;
    ++frequencyCount[db->items[i].salesDate];
  }

  // Compute Run-Length-Encoding with Length Prefix Summing.
  struct RLEDate* RLEDates = malloc(different * sizeof(struct RLEDate));
  if (RLEDates == NULL) {
    exit(1);
  }
  size_t RLEIndex = 0;
  int prefixCount = 0;
  for (size_t i = 0; i <= (size_t)maximum; ++i) {
    if (frequencyCount[i] > 0) {
      prefixCount += frequencyCount[i];
      RLEDates[RLEIndex].date = i;
      RLEDates[RLEIndex].prefixCount = prefixCount;
      ++RLEIndex;
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
  for (size_t i = 0; i < db->itemsCardinality; ++i) {
    orderedItemSalesDate[i] = db->items[i].salesDate;
  }
  // Sort dates.
  qsort(orderedItemSalesDate, db->itemsCardinality, sizeof(int), compare);

  // Find how many different elements are there.
  size_t different = 1;
  for (size_t i = 1; i < db->itemsCardinality; ++i) {
    if (orderedItemSalesDate[i] > orderedItemSalesDate[i - 1]) {
      ++different;
    }
  }

  // Compute Run-Length-Encoding with Length Prefix Summing.
  struct RLEDate* RLEDates = malloc(different * sizeof(struct RLEDate));
  if (RLEDates == NULL) {
    exit(1);
  }
  size_t RLEIndex = 0;
  for (size_t i = 1; i < db->itemsCardinality; ++i) {
    if (orderedItemSalesDate[i] > orderedItemSalesDate[i - 1]) {
      RLEDates[RLEIndex].date = orderedItemSalesDate[i - 1];
      RLEDates[RLEIndex].prefixCount = i;
      ++RLEIndex;
    }
  }
  // Add last elements.
  RLEDates[RLEIndex].date = orderedItemSalesDate[db->itemsCardinality - 1];
  RLEDates[RLEIndex].prefixCount = db->itemsCardinality;

  free(orderedItemSalesDate);

  *RLEDatesCardinality = different;  // == RLEIndex.
  return RLEDates;
}

void* buildQ1Index(void* args) {
  // Q1 index.
  // Clustered index on items. Split the items tuples into NUMBER_OF_BINS bins
  // depending on their price attribute. The first bin contains the tuples
  // with price < maxPrice / 8, the second bin contains the tuples with
  // maxPrice / 8 <= price < 2 * maxPrice / 8 and so on.
  struct ThreadDataBuildIndex* threadData = (struct ThreadDataBuildIndex*)args;
  struct Database* db = threadData->db;

  int pricesRange = db->itemsCardinality / 2;
  size_t binMaxSize = (db->itemsCardinality / NUMBER_OF_BINS) * 2;
  int binRange = pricesRange / NUMBER_OF_BINS;

  // Initialize bins and their sizes.
  struct CompressedItem** itemBins =
      malloc(NUMBER_OF_BINS * sizeof(struct CompressedItem*));
  int* binSizes = calloc(NUMBER_OF_BINS, sizeof(int));
  if (itemBins == NULL || binSizes == NULL) {
    exit(1);
  }
  for (int i = 0; i < NUMBER_OF_BINS; i++) {
    itemBins[i] = malloc(binMaxSize * sizeof(struct CompressedItem));
    if (itemBins[i] == NULL) {
      exit(1);
    }
  }

  // Copy items in corresponding bins.
  for (size_t i = 0; i < db->itemsCardinality; ++i) {
    int binIndex = db->items[i].price / binRange;
    itemBins[binIndex][binSizes[binIndex]++] =
        (struct CompressedItem){.salesDate = db->items[i].salesDate,
                                .employee = db->items[i].employee,
                                .price = db->items[i].price};
  }

  struct Indices* indices = db->indices;
  indices->itemBins = itemBins;
  indices->binSizes = binSizes;

  return NULL;
}

void* buildQ2Index(void* args) {
  // Q2 index.
  // Observation: usually there are many repeated dates, which means that
  // using a Run-Length-Encoding with Length Prefix Summing should help
  // compressing the data a lot.

  // Find out about how sparse the values are in order to use the appropriate
  // sorting function.
  struct ThreadDataBuildIndex* threadData = (struct ThreadDataBuildIndex*)args;
  struct Database* db = threadData->db;

  int minimum = __INT_MAX__;
  int maximum = -1;
  for (size_t i = 0; i < db->itemsCardinality; ++i) {
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

  struct Indices* indices = db->indices;
  indices->RLEDates = RLEDates;
  indices->RLEDatesCardinality = RLEDatesCardinality;
  return NULL;
}

void* buildQ3Index(void* args) {
  // Q3 index.
  // Create a hash table with key `(salesDate, employee)` and value a count
  // representing how many tuples in Items have that particular `(salesDate,
  // employee)` pair. This step is basically a group by `(salesDate, employee)`,
  // performed on items. This is useful as we will use this to compute the final
  // tuple count.
  struct ThreadDataBuildIndex* threadData = (struct ThreadDataBuildIndex*)args;
  struct Database* db = threadData->db;

  // Initialize hash table.
  int itemsCardinality = (int)db->itemsCardinality;
  int salesDateEmployeeToCountCardinality = db->ordersCardinality * 2;
  struct SalesDateEmployeeToCount* salesDateEmployeeToCountHT =
      calloc(salesDateEmployeeToCountCardinality,
             sizeof(struct SalesDateEmployeeToCount));
  if (salesDateEmployeeToCountHT == NULL) {
    exit(1);
  }

  // Populate hash table with counts.
  for (int i = 0; i < itemsCardinality; ++i) {
    struct ItemTuple* itemTuple = &db->items[i];
    int hashValue = hash2(itemTuple->salesDate, itemTuple->employee,
                          salesDateEmployeeToCountCardinality);
    while (salesDateEmployeeToCountHT[hashValue].count > 0 &&
           !(salesDateEmployeeToCountHT[hashValue].salesDate ==
                 itemTuple->salesDate &&
             salesDateEmployeeToCountHT[hashValue].employee ==
                 itemTuple->employee)) {
      // If we have already inserted the pair (salesDate, employee) in the
      // table, we don't need to add it again and the while terminates.
      // This guarantees the uniqueness in the keys of the table.
      hashValue =
          nextSlotLinear(hashValue, salesDateEmployeeToCountCardinality);
    }
    if (salesDateEmployeeToCountHT[hashValue].count == 0) {
      // Adding new pair (salesDate, employee).
      salesDateEmployeeToCountHT[hashValue].salesDate = itemTuple->salesDate;
      salesDateEmployeeToCountHT[hashValue].employee = itemTuple->employee;
    }
    salesDateEmployeeToCountHT[hashValue].count++;
  }

  struct Indices* indices = db->indices;
  indices->salesDateEmployeeToCountHT = salesDateEmployeeToCountHT;
  indices->salesDateEmployeeToCountCardinality =
      salesDateEmployeeToCountCardinality;
  return NULL;
}

void CreateIndices(struct Database* db) {
  // Build indices in parallel. The bottleneck is the index for Q3.
  struct Indices* indices = malloc(sizeof(struct Indices));
  if (indices == NULL) {
    exit(1);
  }
  db->indices = indices;

  struct ThreadDataBuildIndex threadDataQ3 = {.db = db};
  pthread_create(&threadDataQ3.tid, NULL, buildQ3Index, &threadDataQ3);
  struct ThreadDataBuildIndex threadDataQ2 = {.db = db};
  pthread_create(&threadDataQ2.tid, NULL, buildQ2Index, &threadDataQ2);
  struct ThreadDataBuildIndex threadDataQ1 = {.db = db};
  pthread_create(&threadDataQ1.tid, NULL, buildQ1Index, &threadDataQ1);

  indices->sizeStores = pow(2, ceil(log(db->storesCardinality) / log(2)) + 2);

  pthread_join(threadDataQ1.tid, NULL);
  pthread_join(threadDataQ2.tid, NULL);
  pthread_join(threadDataQ3.tid, NULL);
}

void DestroyIndices(struct Database* db) {
  /// Free database indices
  struct Indices* indices = db->indices;
  for (int i = 0; i < NUMBER_OF_BINS; ++i) {
    free(indices->itemBins[i]);
  }
  free(indices->itemBins);
  free(indices->binSizes);
  free(indices->RLEDates);
  free(indices->salesDateEmployeeToCountHT);
  free(indices);
  db->indices = NULL;
}
