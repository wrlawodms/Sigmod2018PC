#pragma once

#include <stdint.h>
#include <stdlib.h>

struct buddy;

struct buddy * buddy_new(int level);
void buddy_delete(struct buddy *);
uint64_t buddy_alloc(struct buddy *, uint64_t size);
void buddy_free(struct buddy *, uint64_t offset);
uint64_t buddy_size(struct buddy *, uint64_t offset);
void buddy_dump(struct buddy *);

