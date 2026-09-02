#include "operations/operation.h"
#include "operations/read.h"

#include <stdlib.h>
#include <string.h>

cbm_operation_result_t cbm_operation_result_take(char *payload, bool is_error) {
    cbm_operation_result_t result = {.payload = payload, .is_error = is_error};
    return result;
}

cbm_operation_result_t cbm_operation_result_copy(const char *payload, bool is_error) {
    const char *text = payload ? payload : "";
    size_t len = strlen(text);
    char *copy = malloc(len + 1);
    if (copy) {
        memcpy(copy, text, len + 1);
    }
    return cbm_operation_result_take(copy, is_error || !copy);
}

void cbm_operation_result_dispose(cbm_operation_result_t *result) {
    if (!result) {
        return;
    }
    free(result->payload);
    result->payload = NULL;
    result->is_error = false;
}

cbm_operation_result_t cbm_operation_execute(cbm_operation_context_t *context,
                                              cbm_operation_id_t operation,
                                              const char *args_json) {
    if (!cbm_operation_descriptor(operation)) {
        return cbm_operation_result_copy("unknown operation", true);
    }
    const char *json = args_json ? args_json : "{}";
    if (cbm_read_operation_supported(operation)) {
        return cbm_read_operation_execute(operation, json);
    }
    if (!context || !context->backend) {
        return cbm_operation_result_copy("operation backend unavailable", true);
    }
    return context->backend(context->backend_context, operation, json);
}
