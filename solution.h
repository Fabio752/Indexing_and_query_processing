#ifndef _SOLUTION_H_
#define _SOLUTION_H_

#include "database.h"
#ifdef __cplusplus
extern "C" {
#endif
int Query1(struct Database* db, int managerID, int price);

int Query2(struct Database* db, int discount, int date);

int Query3(struct Database* db, int countryID);

void CreateIndices(struct Database* db);

void DestroyIndices(struct Database* db);
#ifdef __cplusplus
}
#endif

#endif
