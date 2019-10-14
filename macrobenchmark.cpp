#include "data_generator.h"
#include "database.h"
#include "solution.h"
#include <benchmark/benchmark.h>

static void queryMix(benchmark::State& state) {
  Database db{};
  GenerateData(db, state.range(0));

  std::default_random_engine generator;
  std::uniform_int_distribution<> managerIDs_distribution(0, (db.storesCardinality / 4));
  std::uniform_int_distribution<> prices_distribution(0, (db.itemsCardinality / 2));
  std::uniform_int_distribution<> dates_distribution(0, (db.ordersCardinality / 32));
  std::uniform_int_distribution<> discounts_distribution(0, 100);
  std::uniform_int_distribution<> countryIDs_distribution(0, 196);

  for(auto _ : state) {
    CreateIndices(&db);
    for(auto i = 0; i < 3; i++) {
      benchmark::DoNotOptimize(
          Query1(&db, managerIDs_distribution(generator), prices_distribution(generator)));
      benchmark::DoNotOptimize(
          Query2(&db, discounts_distribution(generator), dates_distribution(generator)));
      benchmark::DoNotOptimize(Query3(&db, countryIDs_distribution(generator)));
    }
    DestroyIndices(&db);
    db.indices = nullptr;
  }
  FreeDatabaseTables(db);
}
BENCHMARK(queryMix)->Arg(2 * 1024 * 1024)->Unit(benchmark::kMicrosecond);
BENCHMARK_MAIN();
