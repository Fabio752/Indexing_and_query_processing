# Advanced Databases coursework

Group members: Fabio Deo (fd5117), Marco Selvatici (ms8817).
Competition name: Nutty Lens Personality.
Score in the competition: 266401
You can find the result of running micro and macro benchmark on our machines in the
file micro_stats.txt.

## Query 1
#### Explanation
This query basically implements a hash join.
Since the conditions applied to the tables filter out many tuples both for orders
and items, we measured that prebuilding hash tables or precalculating joins did not
improve performance. Creating the necessary hash table directly in the query enormously
reduce the number of conflicts you have, since there are much less elements to consider
and the fill-factor stays pretty low.
Instead, we create a clustered index on the items table, by splitting the tuples into
eight bins, depending on their price attribute. The first bin contains the tuples
with `price < maxPrice / 8`, the second bin contains the tuples with
`maxPrice / 8 <= price < 2 * maxPrice / 8` and so on.
When the query is executed, we only lookup the relevant bins trying to perform a
hash join with orders. Furthermore, we parallelize these lookups using 4 threads.
The expected value of bins we have to lookup in a query is 4 (half of 8), since the price
is uniformly distributed in the range `0 -> maxPrice`. We measured a speedup of
around of 2x in the query execution, which is an accord with what we were expecting.

We also tried several other indices, but they never provided an increase in performance.

One of the techniques we tried consisted in partitioning both tables inside
`CreateIndices`, in order to work with each partition in a separate thread.
Doing so resulted in a small loss in performance (running the query on the
raspberry pi gave a score of around 400000, versus the 367680 that we were getting
without this preprocessing).
The code for that can be found in the branch `partitioning2` (a similar partitioning
approach is in the branch `partitioning`).

We also tried to create an unclustered index by sorting the item table by price.
This allows to lookup only the relevant item tuples when executing the query (i.e. tuples
where the price is smaller than the limit set).
Nonetheless, building the index was so slow that the overall macrobenchmark execution
time grew by a factor of four (even if we used RLE encoded prices, sorted with a
counting sort).
The code can be found in the branch `q1_index_sort_prices`.

#### Implementation
Steps marked with an (I) happen when building indices.
1) (I) Partition items in 8 bins.
2) build Orders hash table, using only the relevant tuples.
3) depending on how many bins we have to consider, start a certain number of threads.
   Each thread iterates through one or multiple bins and counts the tuples that can be
   joined with orders (using the hash table built in step 2).
4) Join the threads and return the result.

#### Complexity
- Build indices:
  - time: O(itemsCardinality)
  - space: O(itemsCardinality)
- Execute query:
  - time: O(itemsCardinality + ordersCardinality)
  - space: O(ordersCardinality)


## Query 2
#### Explanation
Our implementation of Q2 is based on the observation that usually the items table
contains many repeated `salesDate`s, and that the `salesDate` is the only information we
care about in items.
This means that Run-Length-Encoding the `salesDate`s in items enormously reduces the
size of the data structure we have too look-up during the query, which translates into
a much shorter execution time (furthermore, the extra memory used by this index tends to be very small).
Since we need to count how many tuples lie in ranges like:
```
items.salesDate <= orders.salesDate <= items.salesDate + x
```
we compute the prefix sum of the Run-Length-Encoded `salesDate`s, which allows us to
use a couple of binary searches in order to reduce the lookup complexity from
O(number_of_different_salesDate) to O(log(number_of_different_salesDates)).

This set of changes gave a speedup of over 1000 times with respect to the initial
naive nested loop join implementation.

#### Implementation
Steps marked with an (I) happen when building indices.
1) (I) Find out how distributed the `items.salesDate` are.
2) (I) Sort `items.salesDate`:
   2.1) if the `salesDate`s are clustered together, use a custom implemented counting sort
        (around 40 times faster than the stdlib qsort).
   2.2) if `salesDate`s distribution makes the use of counting sort impractical, fall
        back to the stdlib qsort.
3) (I) Run length encode the sorted `salesDate`s, and calculate their prefix sum (in the
   implementation, steps 2 and 3 are merged together for performance reasons).
4) Split the range `0 -> ordersCardinality` in `n` parts, based on the number
   of cores available.
5) Run `n` threads, each one iterating through a piece of the orders table and
   counting how many items have a `salesDate` between `order.salesDate - x` and
   `orders.salesDate` (using two binary searches on the RLE index previously built).
6) Join the threads and return the result.

#### Complexity
- Build indices:
  - time:  O(itemsCardinality) [with counting sort]     or
           O(itemsCardinality * log(itemsCardinality)) [with stdlib qsort]
  - space: O(number_of_different_salesDates) [plus O(itemsCardinality) extra memory
                                              freed at the end of the index creation]
- Execute query:
  - time:  O(ordersCardinality * log(number_of_different_salesDates))
  - space: O(1)


## Query 3
Our implementation of Q3 is based on one key observation: a pair `(salesDate, employee)`
in items, is always present also in orders (lines 70 and 71 of `data_generator.h`).
This means that we can group items by pairs of `(salesDate, employee)`
(counting how many prices are there for every such pair) and this table will have
`cardinality <= ordersCardinality`. We do so because usually `ordersCardinality` is much
smaller than `itemsCardinality` and this allows us to save memory and time when executing
the query.

In the query execution, we build a hash table for the stores (which tends to be pretty small).
We then iterate through orders and, for every tuple that can be joined with stores, we increase the
result by looking up how many tuples in items have the same `(salesDate, employee)`.

#### Implementation
Steps marked with an (I) happen when building indices.
1) (I) Create a hash table with key `(salesDate, employee)` and value a count representing
   how many tuples in Items have that particular `(salesDate, employee)` pair.
   This step is basically a group by `(salesDate, employee)`, performed on items.
   This is useful as we will use this to compute the final tuple count.
2) Build the hash table for stores, only considering relevant tuples.
4) Split the range `0 -> ordersCardinality` in `n` parts, based on the number
   of cores available.
5) Run `n` threads, each one iterating through a piece of the orders table and,
   for every tuple that can be joined with stores, we increase the
   result by looking up how many tuples in items have the same `(salesDate, employee)`.
6) Join the threads and return the result.

#### Complexity
- Build indices:
  - time:  O(itemsCardinality)
  - space: O(ordersCardinality)
- Execute query:
  - time:  O(storesCardinality + ordersCardinality)
  - space: O(storesCardinality)


## Other optimizations
In order to maximize performance, we also considered optimizations like:
- We build all the indices in parallel (this way the execution time of `CreateIndices` is only
  the time required to build the slowest index).
- Use smaller datatypes when possible. By looking at the code in `data_generator.h`, you
  can find out that some fields will never exceed the size of a 16 bit integer.
  This improves memory efficiency, while also reducing the number of cache misses.
- Use bitwise-and (`&`) instead of the modulo operation (`%`) when performing hashing or probing.
  In general, a modulo operation requires a division to be performed, which can become
  quite costly if it gets executed millions of times. Using a bitwise-and is much cheaper.
  In order to get this optimization to work well, we make sure the sizes of all hash
  tables are powers of 2. If that is the case, and-ing with `size - 1` has the same
  effect as performing a `% size` operation.
  Since the hash and probing functions were heavily used, this provided a 5-10% speedup on the
  overall execution time.
- Use `calloc` instead of `malloc` followed by a `memset`. 
- Use pointer arithmetics in order to speed up loops. Unfortunately this made the code
  slightly messier without giving a significant performance improvement, so it has been
  reverted.

## Extra
There are many parts of our solution that we believe can qualify as extra:
- The use of partitioned parallel probing of hash tables in all queries.
- The use of binning in query 1, that allowed us to score below 300000 on labTS.
- The use of adaptive code that builds Q2 index in different ways depending
  on the data distribution.
- The very good performance in the labTS competition.
- The other optimizations that played an important role in making our solution much more
  time and memory efficient.
