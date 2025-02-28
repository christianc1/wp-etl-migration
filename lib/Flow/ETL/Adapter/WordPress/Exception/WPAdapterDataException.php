<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\Exception;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;

/**
 * Exception thrown when row data doesn't satisfy requirements for WordPress operations.
 *
 * This exception is used when the data provided doesn't meet the requirements
 * for a WordPress operation, such as missing required fields or invalid data formats.
 */
class WPAdapterDataException extends RuntimeException
{
    /**
     * @var array|Row|null The problematic data if available
     */
    private $data;

    /**
     * @var string|null The field name that caused the issue
     */
    private ?string $fieldName;

    /**
     * Create a new exception for missing required data.
     *
     * @param string $fieldName The name of the missing or invalid field
     * @param array|Row|null $data The problematic data
     * @return self
     */
    public static function missingRequiredData(string $fieldName, $data = null): self
    {
        $exception = new self("Required field '{$fieldName}' is missing or invalid");
        $exception->fieldName = $fieldName;
        $exception->data = $data;

        return $exception;
    }

    /**
     * Create a new exception for invalid data format.
     *
     * @param string $fieldName The name of the field with invalid format
     * @param string $expectedFormat Description of the expected format
     * @param array|Row|null $data The problematic data
     * @return self
     */
    public static function invalidDataFormat(string $fieldName, string $expectedFormat, $data = null): self
    {
        $exception = new self("Field '{$fieldName}' has invalid format. Expected: {$expectedFormat}");
        $exception->fieldName = $fieldName;
        $exception->data = $data;

        return $exception;
    }

    /**
     * Get the problematic data if available.
     *
     * @return array|Row|null
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Get the field name that caused the issue.
     *
     * @return string|null
     */
    public function getFieldName(): ?string
    {
        return $this->fieldName;
    }
}
