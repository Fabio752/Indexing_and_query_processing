#include <benchmark/benchmark.h>

#include "data_generator.h"
#include "database.h"
#include "solution.h"

static void CreateIndicesBenchmark(benchmark::State& state) {
  Database db{};
  GenerateData(db, state.range(0));

  for(auto _ : state) {
    CreateIndices(&db);
    state.PauseTiming();
    DestroyIndices(&db);
    db.indices = nullptr;
    state.ResumeTiming();
  }

  FreeDatabaseTables(db);
}
BENCHMARK(CreateIndicesBenchmark)->Range(1024, 1024 * 1024)->Unit(benchmark::kMicrosecond);

static void Query1Benchmark(benchmark::State& state) {
  Database db{};
  GenerateData(db, state.range(0));
  CreateIndices(&db);

  std::default_random_engine generator;
  std::uniform_int_distribution<> managerIDs_distribution(0, (db.storesCardinality / 4));
  std::uniform_int_distribution<> prices_distribution(0, (db.itemsCardinality / 2));

  for(auto _ : state)
    for(auto i = 0; i < 10; i++)
      benchmark::DoNotOptimize(
          Query1(&db, managerIDs_distribution(generator), prices_distribution(generator)));

  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}
BENCHMARK(Query1Benchmark)->Range(1024, 1024 * 1024)->Unit(benchmark::kMicrosecond);

static void Query2Benchmark(benchmark::State& state) {
  Database db{};
  GenerateData(db, state.range(0));
  CreateIndices(&db);

  std::default_random_engine generator;
  std::uniform_int_distribution<> dates_distribution(0, (db.ordersCardinality / 32));
  std::uniform_int_distribution<> discounts_distribution(0, 100);

  for(auto _ : state)
    for(auto i = 0; i < 10; i++)
      benchmark::DoNotOptimize(
          Query2(&db, dates_distribution(generator), discounts_distribution(generator)));

  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}
BENCHMARK(Query2Benchmark)->Range(1024, 512 * 1024)->Unit(benchmark::kMicrosecond);

static void Query3Benchmark(benchmark::State& state) {
  Database db{};
  GenerateData(db, state.range(0));
  CreateIndices(&db);

  std::default_random_engine generator;
  std::uniform_int_distribution<> countryIDs_distribution(0, 196);

  for(auto _ : state)
    for(auto i = 0; i < 10; i++)
      benchmark::DoNotOptimize(Query3(&db, countryIDs_distribution(generator)));

  DestroyIndices(&db);
  db.indices = nullptr;
  FreeDatabaseTables(db);
}
BENCHMARK(Query3Benchmark)->Range(1024, 256 * 1024)->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();
