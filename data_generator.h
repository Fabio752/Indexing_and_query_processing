#ifndef _DATA_GENERATOR_H_
#define _DATA_GENERATOR_H_

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <random>

#include "database.h"

namespace cppref {
using namespace std;
// implementation taken from cpp-ref (just to be portable)
template <class RandomIt, class URBG> void shuffle(RandomIt first, RandomIt last, URBG&& g) {
  auto n = last - first;
  for(auto i = n - 1; i > 0; --i) {
    std::swap(first[i], first[g() % n]); // ignoring modulo skew for the sake of portablility
  }
}
} // namespace cppref

template <typename Generator = std::linear_congruential_engine<unsigned int, 16807, 0, 2147483647>>
void GenerateData(Database& db, size_t itemsCardinality = 16384) {
  Generator lcg(221);
  auto averageNumberOfItemsPerOrder = 4ul;
  auto numberOfSalesPerDay = 32;
  auto numberOfStores = 256 * std::lround(std::log2(itemsCardinality));

  auto numberOfUniqueManagers = numberOfStores / 4; // four stores per manager
  auto numberOfUniqueCountries = 196;
  auto numberOfUniqueEmployees = numberOfUniqueManagers * 8;
  auto numberOfUniquePrices = itemsCardinality / 2;

  db.itemsCardinality = itemsCardinality;
  db.ordersCardinality = itemsCardinality / averageNumberOfItemsPerOrder;
  db.storesCardinality = numberOfStores;

  db.items = new ItemTuple[db.itemsCardinality];
  db.orders = new OrderTuple[db.ordersCardinality];
  db.stores = new StoreTuple[db.storesCardinality];

  for(auto i = 0ul; i < db.storesCardinality; i++) {
    db.stores[i].managerID = lcg() % numberOfUniqueManagers;
    db.stores[i].countryID = lcg() % numberOfUniqueCountries;
    db.stores[i].longitude = lcg();
    db.stores[i].latitude = lcg();
  }
  for(auto i = 0ul; i < db.ordersCardinality; i+=2) {
    auto salesDate = (i / numberOfSalesPerDay) + lcg() % 2;

    for (size_t j = 0; j < 2; j++)
    {
      db.orders[i + j].salesDate = salesDate;
      db.orders[i + j].employee = i % numberOfUniqueEmployees; 
      db.orders[i + j].employeeManagerID = db.stores[lcg() % db.storesCardinality].managerID;
      db.orders[i + j].discount = (lcg()) % 100;  
    }
  }

  cppref::shuffle(db.orders, db.orders + db.ordersCardinality - 1, lcg);

  auto itemsCursor = 0ul;
  for(size_t i = 0ul; i < db.ordersCardinality; i++) {
    auto v = lcg();
    int numberOfItemsPerOrderVariation = ((v & 2)>>1) + (v & 1) - 1;
    size_t numberOfItemsPerOrder =
        averageNumberOfItemsPerOrder + numberOfItemsPerOrderVariation; // fix this
    for(size_t j = 0; (j < numberOfItemsPerOrder) && (itemsCursor < db.itemsCardinality); j++) {
      db.items[itemsCursor].salesDate = db.orders[i].salesDate;
      db.items[itemsCursor].employee = db.orders[i].employee;
      db.items[itemsCursor].price = lcg() % numberOfUniquePrices;
      itemsCursor++;
    }
  }

  std::lognormal_distribution<double> distribution(.3, .8);
  for(size_t i = 0; i < db.itemsCardinality; i++)
    std::swap(
        db.items[i],
        db.items[std::min(db.itemsCardinality, static_cast<size_t>(floorl(distribution(lcg))))]);
}

void FreeDatabaseTables(struct Database& db) {
  delete[] db.items;
  delete[] db.orders;
  delete[] db.stores;
}

#endif
