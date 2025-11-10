package io.xrex.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for storing the result of a request for idempotency purposes.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IdempotencyRecordDto {
    /**
     * The status of the processed request, e.g., "SUCCESS", "FAILED".
     */
    private String status;

    /**
     * The response data to be returned to the client, serialized as a String.
     * This could be a serialized TransferResponse or an error message.
     */
    private String responseData;
}
