#ifndef DATABASE_H_
#define DATABASE_H_

#include <stdlib.h>

struct ItemTuple
{
    int salesDate;
    int employee;
    int price;
};

struct OrderTuple
{
    int salesDate;
    int employee;
    int employeeManagerID;
    int discount;
};

struct StoreTuple
{
    int managerID;
    int latitude;
    int longitude;
    int countryID;
};

struct Database {
    struct ItemTuple*  items;
    size_t itemsCardinality;
    struct OrderTuple* orders;
    size_t ordersCardinality;
    struct StoreTuple* stores;
    size_t storesCardinality;
    void* indices;
};


#endif
