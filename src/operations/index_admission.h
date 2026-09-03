#ifndef CBM_OPERATIONS_INDEX_ADMISSION_H
#define CBM_OPERATIONS_INDEX_ADMISSION_H

#include <stdbool.h>

enum { CBM_DEFAULT_AUTO_INDEX_LIMIT = 50000 };

bool cbm_auto_index_within_file_limit(const char *root_path, int file_limit,
                                      int *file_count_out);

#endif
