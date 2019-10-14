#define CATCH_CONFIG_MAIN
#include "data_generator.h"
#include "database.h"
#include "solution.h"
#include <catch2/catch.hpp>

int managerIDs[10] = {7, 26, 103, 14, 77, 42, 54, 112, 5, 115};
int prices[10] = {200, 1205, 7221, 6590, 1800, 750, 968, 3500, 4550, 5225};
int discounts[10] = {10, 5, 80, 90, 70, 40, 55, 85, 7, 35};
int dates[10] = {2, 12, 7, 21, 18, 5, 13, 15, 8, 11};
int countryIDs[10] = {5, 105, 41, 52, 70, 38, 191, 11, 7, 39};

int query1_results[10] = {2, 9, 32, 14, 22, 5, 5, 8, 53, 21};
int query2_results[10] = {16687, 73210, 46038, 64247, 86670, 33833, 63174, 72352, 53691, 51191};
int query3_results[10] = {468, 1277, 801, 665, 950, 1118, 500, 571, 979, 709};

TEST_CASE("Queries 1 Solution works", "[queries]") {
  Database db;
  GenerateData(db);
  CreateIndices(&db);
  auto i = GENERATE(range(0, 10));
  REQUIRE(Query1(&db, managerIDs[i], prices[i]) == query1_results[i]);
  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}

TEST_CASE("Queries 2 Solution works", "[queries]") {
  Database db;
  GenerateData(db);
  CreateIndices(&db);
  auto i = GENERATE(range(0, 10));
  REQUIRE(Query2(&db, discounts[i], dates[i]) == query2_results[i]);
  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}

TEST_CASE("Queries 3 Solution works", "[queries]") {
  Database db;
  GenerateData(db);
  CreateIndices(&db);
  auto i = GENERATE(range(0, 10));
  REQUIRE(Query3(&db, countryIDs[i]) == query3_results[i]);
  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}
